use anyhow::Context;
use futures::{channel::mpsc, FutureExt, StreamExt};
use js_sys::{Array, ArrayBuffer, Uint8Array};
use log::info;
use radixdb::Blob;
use range_collections::{AbstractRangeSet, RangeSet, RangeSet2};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::{TryFrom, TryInto},
    ops::{Bound, Range},
    str::FromStr,
};
use url::Url;
use wasm_bindgen::JsValue;
use web_sys::{Cache, CacheQueryOptions, Request, Response};

use crate::{
    contains, js_async, overlap, ranges, sync_fs::SyncFs, worker_scope, Command, JsError, SharedStr,
};

/// A virtual file system that uses the browser cache as storage
pub struct WebCacheFs {
    dirs: BTreeMap<SharedStr, (WebCacheDir, BTreeMap<SharedStr, WebCacheFile>)>,
    queue: mpsc::UnboundedReceiver<Command>,
}

impl WebCacheFs {
    /// #[cfg(target_arch = "wasm32")]
    /// create a new WebCacheFs on a separate thread, and wrap it in a SyncFs
    pub fn new(pool: wasm_futures_executor::ThreadPool) -> SyncFs {
        let (tx, rx) = mpsc::unbounded();
        pool.spawn_lazy(|| {
            Self {
                queue: rx,
                dirs: Default::default(),
            }
            .run()
            .boxed_local()
        });
        SyncFs::new(tx)
    }

    async fn open_dir(&mut self, dir_name: impl Into<SharedStr>) -> anyhow::Result<()> {
        let dir_name = dir_name.into();
        if !self.dirs.contains_key(&dir_name) {
            let dir = WebCacheDir::new(&dir_name).await?;
            self.dirs.insert(dir_name, (dir, Default::default()));
        }
        Ok(())
    }

    async fn open_file(&mut self, dir_name: SharedStr, file_name: SharedStr) -> anyhow::Result<()> {
        let (dir, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        if !files.contains_key(&file_name) {
            let wcf = dir.open_file(file_name.clone()).await?;
            files.insert(file_name, wcf);
        }
        Ok(())
    }

    async fn file_exists(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
    ) -> anyhow::Result<bool> {
        let (dir, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        if !files.contains_key(&file_name) {
            dir.file_exists(file_name.clone()).await
        } else {
            Ok(true)
        }
    }

    async fn append_file_length_prefixed(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.append_length_prefixed(data).await
    }

    async fn flush_file(&mut self, dir: SharedStr, file_name: SharedStr) -> anyhow::Result<()> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.flush().await
    }

    async fn read_file_length_prefixed(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
        offset: u64,
    ) -> anyhow::Result<Blob<u8>> {
        let (_, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.load_length_prefixed(offset).await
    }

    async fn delete_file(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
    ) -> anyhow::Result<bool> {
        let (dir, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let res = dir.delete(&file_name).await?;
        files.remove(&file_name);
        Ok(res)
    }

    async fn delete_dir(&mut self, dir_name: SharedStr) -> anyhow::Result<bool> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let res: JsValue = js_async(caches.delete(&dir_name)).await?;
        Ok(res.as_bool().context("no bool")?)
    }

    async fn read(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
        range: Range<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        let (_, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.read(range).await
    }

    async fn write(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
        offset: u64,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let (_, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.write(offset, data).await
    }

    async fn length(&mut self, dir_name: SharedStr, file_name: SharedStr) -> anyhow::Result<u64> {
        let (_, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.length().await
    }

    async fn truncate(
        &mut self,
        dir_name: SharedStr,
        file_name: SharedStr,
        size: u64,
    ) -> anyhow::Result<()> {
        let (_, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.truncate(size).await
    }

    async fn run(mut self) {
        while let Some(cmd) = self.queue.next().await {
            match cmd {
                Command::OpenDir { dir_name, cb } => {
                    let _ = cb.send(self.open_dir(dir_name).await);
                }
                Command::DeleteDir { dir_name, cb } => {
                    let _ = cb.send(self.delete_dir(dir_name).await);
                }
                Command::Read {
                    dir_name,
                    file_name,
                    range,
                    cb,
                } => {
                    let _ = cb.send(self.read(dir_name, file_name, range).await);
                }
                Command::Write {
                    dir_name,
                    file_name,
                    offset,
                    data,
                    cb,
                } => {
                    let _ = cb.send(self.write(dir_name, file_name, offset, data).await);
                }
                Command::Length {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.length(dir_name, file_name).await);
                }
                Command::Truncate {
                    dir_name,
                    file_name,
                    size,
                    cb,
                } => {
                    let _ = cb.send(self.truncate(dir_name, file_name, size).await);
                }
                Command::ReadFileLengthPrefixed {
                    dir_name,
                    file_name,
                    offset,
                    cb,
                } => {
                    let _ = cb.send(
                        self.read_file_length_prefixed(dir_name, file_name, offset)
                            .await,
                    );
                }
                Command::AppendFileLengthPrefixed {
                    dir_name,
                    file_name,
                    data,
                    cb,
                } => {
                    let _ = cb.send(
                        self.append_file_length_prefixed(dir_name, file_name, data)
                            .await,
                    );
                }
                Command::FlushFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.flush_file(dir_name, file_name).await);
                }
                Command::OpenFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.open_file(dir_name, file_name).await);
                }
                Command::FileExists {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.file_exists(dir_name, file_name).await);
                }
                Command::DeleteFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.delete_file(dir_name, file_name).await);
                }
                Command::Shutdown => {
                    // shut down the loop,
                    // which will shut down the thread,
                    // which will drop the mpsc receiver,
                    // which will cause all file ops to fail.
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebCacheDir {
    /// the cache which we abuse as a directory
    cache: web_sys::Cache,
}

impl WebCacheDir {
    pub(crate) async fn new(dir_name: &str) -> anyhow::Result<Self> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let cache = js_async(caches.open(dir_name)).await?;
        Ok(Self { cache })
    }

    pub(crate) async fn open_file(&self, file_name: SharedStr) -> anyhow::Result<WebCacheFile> {
        WebCacheFile::new(self.cache.clone(), file_name).await
    }

    pub(crate) async fn file_exists(&self, file_name: SharedStr) -> anyhow::Result<bool> {
        let chunks = chunks(&self.cache, &file_name).await?;
        Ok(!chunks.is_empty())
    }

    pub(crate) async fn delete(&self, file_name: &str) -> anyhow::Result<bool> {
        let res: JsValue = js_async(
            self.cache.delete_with_str_and_options(
                &format!("/{}", file_name),
                CacheQueryOptions::new()
                    .ignore_method(true)
                    .ignore_search(true),
            ),
        )
        .await?;
        Ok(res.as_bool().context("not a bool")?)
    }
}

#[derive(Debug)]
pub struct WebCacheFile {
    /// the cache (basically the directory where the file lives in)
    cache: web_sys::Cache,
    /// the file name
    file_name: SharedStr,
    /// all the chunks that we have, sorted
    chunks: BTreeSet<ChunkMeta>,
}

impl WebCacheFile {
    pub(crate) async fn new(cache: web_sys::Cache, file_name: SharedStr) -> anyhow::Result<Self> {
        let chunks = chunks(&cache, &file_name).await?;
        info!("opened file {:?} {} {:?}", cache, file_name, chunks);
        // let to_delete = retain_relevant(&mut chunks);
        // delete(&cache, &file_name, &to_delete).await?;
        Ok(Self {
            cache,
            file_name,
            chunks,
        })
    }

    fn prev(&self) -> ChunkMeta {
        self.chunks.iter().last().cloned().unwrap_or_default()
    }

    pub(crate) async fn flush(&mut self) -> anyhow::Result<()> {
        // simple fs flushes after each op
        Ok(())
    }

    pub(crate) async fn append(&mut self, data: &mut Vec<u8>) -> anyhow::Result<u64> {
        let prev = self.prev();
        let offset = prev.range().end;
        let plan = add(&self.chunks, offset, data.clone());
        execute_plan(&self.cache, &self.file_name, plan, &mut self.chunks).await?;
        Ok(offset)
    }

    pub(crate) async fn append_length_prefixed(
        &mut self,
        mut data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        data.splice(0..0, u32::try_from(data.len()).unwrap().to_be_bytes());
        self.append(&mut data).await
    }

    async fn load_chunks(
        &self,
        range: Range<u64>,
        chunks: impl IntoIterator<Item = &ChunkMeta>,
    ) -> anyhow::Result<Vec<u8>> {
        // let t0 = js_sys::Date::now();
        let len = range.end.saturating_sub(range.start).try_into()?;
        let mut res = vec![0u8; len];
        for chunk in chunks.into_iter() {
            let (sr, tr) = ranges(chunk.range(), range.clone())?;
            // let t0 = js_sys::Date::now();
            let data = load_range(&self.cache, &self.file_name, chunk, sr).await?;
            // info!("load_range {}", js_sys::Date::now() - t0);
            data.copy_to(&mut res[tr]);
        }
        // info!("load_chunks {}", js_sys::Date::now() - t0);
        Ok(res)
    }

    pub(crate) async fn load_length_prefixed(&self, start: u64) -> anyhow::Result<Blob<u8>> {
        // info!("load_length_prefixed index={}", start);
        // let t0 = js_sys::Date::now();
        let chunks = intersecting_chunks(&self.chunks, start..start + 4).collect::<Vec<_>>();
        let end = chunks.iter().map(|x| x.range().end).max().unwrap_or(start);
        let res = self.load_chunks(start..end, chunks).await?;
        let len = usize::try_from(u32::from_be_bytes(res[0..4].try_into().unwrap()))?;
        anyhow::ensure!(res.len() >= len + 4);
        let res = Blob::arc_from_byte_slice(&res[4..len + 4]);
        // info!("load_length_prefixed {}", js_sys::Date::now() - t0);
        Ok(res)
    }

    pub(crate) async fn read(&self, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        let len_u64 = range.end.saturating_sub(range.start);
        let len: usize = len_u64.try_into()?;
        anyhow::ensure!(
            range.end <= self.prev().range().end,
            "tried to read beyond the end of the file"
        );
        let mut res = vec![0u8; len];
        for chunk in intersecting_chunks(&self.chunks, range.clone()) {
            let (sr, tr) = ranges(chunk.range(), range.clone())?;
            let data = load_range(&self.cache, &self.file_name, chunk, sr).await?;
            data.copy_to(&mut res[tr]);
        }
        Ok(res)
    }

    pub(crate) async fn write(&mut self, offset: u64, mut data: Vec<u8>) -> anyhow::Result<()> {
        let plan = add(&self.chunks, offset, data);
        execute_plan(&self.cache, &self.file_name, plan, &mut self.chunks).await?;
        Ok(())
    }

    pub(crate) async fn length(&mut self) -> anyhow::Result<u64> {
        Ok(self.prev().range().end)
    }

    pub(crate) async fn truncate(&mut self, offset: u64) -> anyhow::Result<()> {
        let plan = truncate(&self.chunks, offset);
        execute_plan(&self.cache, &self.file_name, plan, &mut self.chunks).await?;
        Ok(())
    }

    async fn load(&self, chunk: &ChunkMeta) -> anyhow::Result<Vec<u8>> {
        let request: String = chunk.to_request_str(&self.file_name);
        let response: Response = js_async(self.cache.match_with_str(&request)).await?;
        let ab: ArrayBuffer = js_async(response.array_buffer().map_err(JsError::from)?).await?;
        Ok(Uint8Array::new(&ab).to_vec())
    }

    fn mk_chunk(&self, range: Range<u64>) -> ChunkMeta {
        let overwrite = intersecting_chunks(&self.chunks, range.clone())
            .next()
            .is_some();
        let level = if overwrite {
            self.prev().level + 1
        } else {
            self.prev().level
        };
        ChunkMeta {
            level,
            offset: range.start,
            len: range.end.saturating_sub(range.start),
        }
    }
}

fn intersecting_chunks<'a>(
    chunks: impl IntoIterator<Item = &'a ChunkMeta>,
    range: Range<u64>,
) -> impl Iterator<Item = &'a ChunkMeta> {
    chunks
        .into_iter()
        .filter(move |chunk| overlap(chunk.range(), range.clone()))
}

/// executes a plan consisting of a number of ops that have to be executed in order
async fn execute_plan(
    cache: &web_sys::Cache,
    file_name: &str,
    plan: Vec<Op>,
    chunks: &mut BTreeSet<ChunkMeta>,
) -> anyhow::Result<()> {
    info!("plan {:#?}", plan);
    for op in plan {
        match op {
            Op::Delete(chunk) => {
                let request = chunk.to_request_str(file_name);
                let _: JsValue = js_async(cache.delete_with_str(&request)).await?;
                chunks.remove(&chunk);
            }
            Op::Write(chunk, mut data) => {
                write_chunk(cache, file_name, &chunk, &mut data).await?;
                chunks.insert(chunk);
            }
            Op::Rewrite(chunk) => {
                let mut data = read(cache, file_name, chunks, chunk.range()).await?;
                write_chunk(cache, file_name, &chunk, &mut data).await?;
                chunks.insert(chunk);
            }
        }
    }
    Ok(())
}

async fn read(
    cache: &web_sys::Cache,
    file_name: &str,
    chunks: &BTreeSet<ChunkMeta>,
    range: Range<u64>,
) -> anyhow::Result<Vec<u8>> {
    let len_u64 = range.end.saturating_sub(range.start);
    let len: usize = len_u64.try_into()?;
    let mut res = vec![0u8; len];
    for chunk in intersecting_chunks(chunks.iter(), range.clone()) {
        let (sr, tr) = ranges(chunk.range(), range.clone())?;
        let data = load_range(cache, file_name, chunk, sr).await?;
        data.copy_to(&mut res[tr]);
    }
    Ok(res)
}

async fn load_range(
    cache: &web_sys::Cache,
    file_name: &str,
    chunk: &ChunkMeta,
    range: Range<usize>,
) -> anyhow::Result<Uint8Array> {
    let request: String = chunk.to_request_str(file_name);
    // let t0 = js_sys::Date::now();
    let response: Response = js_async(cache.match_with_str(&request)).await?;
    // info!("get response {}", js_sys::Date::now() - t0);
    const USE_BLOB: bool = true;
    let ab = if USE_BLOB {
        // let t0 = js_sys::Date::now();
        let blob: web_sys::Blob = js_async(response.blob().map_err(JsError::from)?).await?;
        // info!("load blob {}", js_sys::Date::now() - t0);
        // let t0 = js_sys::Date::now();
        let slice = blob
            .slice_with_i32_and_i32(range.start.try_into()?, range.end.try_into()?)
            .map_err(JsError::from)?;
        // info!("slice blob {}", js_sys::Date::now() - t0);
        // let t0 = js_sys::Date::now();
        let res: ArrayBuffer = js_async(slice.array_buffer()).await?;
        // info!("blob -> arraybuffer {}", js_sys::Date::now() - t0);
        res
    } else {
        let ab: ArrayBuffer = js_async(response.array_buffer().map_err(JsError::from)?).await?;
        ab.slice_with_end(range.start.try_into()?, range.end.try_into()?)
    };
    // let t0 = js_sys::Date::now();
    let ua = Uint8Array::new(&ab);
    // info!("convert to uint8array {}", js_sys::Date::now() - t0);
    Ok(ua)
}

async fn delete(
    cache: &web_sys::Cache,
    file_name: &str,
    to_delete: &[ChunkMeta],
) -> anyhow::Result<()> {
    for chunk in to_delete {
        let request = chunk.to_request_str(file_name);
        let _: JsValue = js_async(cache.delete_with_str(&request)).await?;
    }
    Ok(())
}

async fn write_chunk(
    cache: &Cache,
    file_name: &str,
    chunk: &ChunkMeta,
    data: &mut [u8],
) -> anyhow::Result<()> {
    let request = chunk.to_request_str(file_name);
    let response = Response::new_with_opt_u8_array(Some(data)).map_err(JsError::from)?;
    let _: JsValue = js_async(cache.put_with_str(&request, &response)).await?;
    Ok(())
}

async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<BTreeSet<ChunkMeta>> {
    // todo: is there a way to do this without getting all keys?
    let keys: JsValue = js_async(cache.keys()).await?;
    let chunks = Array::from(&keys)
        .iter()
        .filter_map(|req| ChunkMeta::parse_and_filter(&Request::from(req).url(), name).ok())
        .filter_map(|x| x)
        .collect();
    Ok(chunks)
}

/// sort chunks, retain relevant, and return the ones to be deletes as a vec
fn retain_relevant(chunks: &mut Vec<ChunkMeta>) -> Vec<ChunkMeta> {
    chunks.sort();
    let mut prev = ChunkMeta::default();
    let mut to_delete = Vec::new();
    chunks.retain(|chunk| {
        if prev.level == chunk.level {
            // same level as previous
            if prev.range().end == chunk.range().start {
                // consecutive, move ahead
                prev = *chunk;
                true
            } else {
                // non-consecutive, discard
                to_delete.push(*chunk);
                false
            }
        } else if prev.level + 1 == chunk.level {
            if chunk.range().start <= prev.range().end {
                // consecutive with previous level, take
                prev = *chunk;
                true
            } else {
                // chunk is not consecutive, discard
                to_delete.push(*chunk);
                false
            }
        } else {
            to_delete.push(*chunk);
            false
        }
    });
    to_delete
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
/// metadata for a chunk
struct ChunkMeta {
    /// level - gets increased on truncate
    ///
    /// this is the only way to do atomic truncate with a non atomic blob store like the web-sys cache
    level: u64,
    /// offset
    offset: u64,
    /// len
    len: u64,
}

impl ChunkMeta {
    fn new(range: Range<u64>, level: u64) -> Self {
        Self {
            level,
            offset: range.start,
            len: range.end - range.start,
        }
    }

    fn range(&self) -> Range<u64> {
        self.offset..(self.offset + self.len)
    }

    fn parse_and_filter(request: &str, file_name: &str) -> anyhow::Result<Option<ChunkMeta>> {
        // info!("parse_and_filter {}", request);
        let url = Url::parse(request)?;
        if let Some(query) = url.query() {
            let name = url.path().strip_prefix('/').unwrap();
            // info!("parse_and_filter name {} file_name {}  query {}", name, file_name, query);
            if name != file_name {
                // not ours
                Ok(None)
            } else {
                let meta = ChunkMeta::from_str(query)?;
                // info!("chunk {:?}", meta);
                Ok(Some(meta))
            }
        } else {
            Ok(None)
        }
    }

    fn to_request_str(&self, file_name: &str) -> String {
        format!(
            "/{}?{:04x}-{:08x}-{:04x}",
            file_name, self.level, self.offset, self.len
        )
    }
}

impl ToString for ChunkMeta {
    fn to_string(&self) -> String {
        format!("{:04x}-{:08x}-{:04x}", self.level, self.offset, self.len)
    }
}

impl FromStr for ChunkMeta {
    type Err = anyhow::Error;

    fn from_str(args: &str) -> Result<Self, Self::Err> {
        let parts = args.split('-').collect::<Vec<&str>>();
        anyhow::ensure!(parts.len() == 3);
        let level = u64::from_str_radix(parts[0], 16)?;
        let offset = u64::from_str_radix(parts[1], 16)?;
        let len = u64::from_str_radix(parts[2], 16)?;
        Ok(Self { level, offset, len })
    }
}

#[derive(PartialEq, Eq, Clone)]
enum Op {
    // read the range and then write it again with the given level
    Rewrite(ChunkMeta),
    // delete the given range+level
    Delete(ChunkMeta),
    // write new data with the given range+level
    Write(ChunkMeta, Vec<u8>),
}

impl std::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rewrite(arg0) => f.debug_tuple("Rewrite").field(arg0).finish(),
            Self::Delete(arg0) => f.debug_tuple("Delete").field(arg0).finish(),
            Self::Write(arg0, arg1) => f
                .debug_tuple("Write")
                .field(arg0)
                .field(&arg1.len())
                .finish(),
        }
    }
}

// produces the sequence of ops to atomically add a chunk
fn add(ranges: &BTreeSet<ChunkMeta>, offset: u64, data: Vec<u8>) -> Vec<Op> {
    let range = offset..offset + (data.len() as u64);
    // all existing chunks that are relevant for the new chunk
    let relevant = ranges
        .iter()
        .filter(|x| overlap(x.range(), range.clone()))
        .collect::<Vec<_>>();
    // max level for the new chunk
    let level = relevant
        .iter()
        .map(|x| x.level)
        .max()
        .map(|x| x + 1)
        .unwrap_or_default();
    let (rewrite, delete): (Vec<ChunkMeta>, Vec<ChunkMeta>) = relevant
        .into_iter()
        .partition(|x| !contains(range.clone(), x.range()));
    let mut res = Vec::new();
    // first do the write, with a level that puts the new data above all existing
    res.push(Op::Write(ChunkMeta::new(range.clone(), level), data));
    // pieces that are completely overlapped by the new chunk can be deleted
    res.extend(delete.into_iter().map(Op::Delete));
    for r in rewrite {
        if r.range().start < range.start {
            res.push(Op::Rewrite(ChunkMeta::new(
                r.range().start..range.start,
                level,
            )));
        }
        if r.range().end > range.end {
            res.push(Op::Rewrite(ChunkMeta::new(range.end..r.range().end, level)));
        }
        res.push(Op::Delete(r));
    }
    res
}

// produces a sequence of ops to truncate a file
fn truncate(ranges: &BTreeSet<ChunkMeta>, offset: u64) -> Vec<Op> {
    // all existing chunks that are relevant for the range offset..
    let relevant = ranges
        .iter()
        .filter(|x| x.range().end > offset)
        .cloned()
        .collect::<Vec<_>>();
    // max level for the new chunk, if any
    let level = relevant
        .iter()
        .map(|x| x.level)
        .max()
        .map(|x| x + 1)
        .unwrap_or_default();
    let mut res = Vec::new();
    // if some chunks intersect the truncate offset, rewrite before them
    if let Some(start) = relevant.iter().map(|x| x.range().start).min() {
        if start < offset {
            res.push(Op::Rewrite(ChunkMeta::new(start..offset, level)));
        }
    }
    // delete all relevant chunks
    res.extend(relevant.into_iter().map(Op::Delete));
    res
}

fn get_file_range(iter: impl IntoIterator<Item = Range<u64>>) -> Range<u64> {
    let ranges = iter.into_iter().fold(RangeSet2::empty(), |mut a, b| {
        a.union_with(&RangeSet2::from(b));
        a
    });
    match ranges.iter().next() {
        Some((Bound::Included(start), Bound::Excluded(end))) if *start == 0 => *start..*end,
        _ => 0..0,
    }
}

fn open(chunks: &BTreeSet<ChunkMeta>) -> Vec<Op> {
    type RangeSet = range_collections::RangeSet<[u64; 2]>;
    let mut res = vec![];
    let level = chunks
        .iter()
        .map(|x| x.level)
        .max()
        .map(|x| x + 1)
        .unwrap_or_default();

    // figure out the actual range of the file - until the first gap. If there is a gap for the first piece, the file is empty.
    let file_range: RangeSet = get_file_range(chunks.iter().map(|x| x.range())).into();
    let mut covered = RangeSet::empty();
    for chunk in chunks.iter().rev() {
        let chunk_range = RangeSet::from(chunk.range());
        if chunk_range.is_disjoint(&file_range) {
            // delete the chunk, it is behind the truncate
            res.push(Op::Delete(*chunk))
        } else {
            let remaining: RangeSet = chunk_range.difference(&covered);
            if remaining != chunk_range {
                // some parts are already covered
                // rewrite the slices
                for range in remaining.iter() {
                    if let (Bound::Included(start), Bound::Excluded(end)) = range {
                        res.push(Op::Rewrite(ChunkMeta::new(*start..*end, level)));
                    }
                }
                // delete the chunk
                res.push(Op::Delete(*chunk))
            }
            covered.union_with(&chunk_range);
        }
    }
    res
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::web_cache_fs::Op;

    use super::{add, open, truncate, ChunkMeta};

    #[test]
    fn add_ops() {
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(100..200, 0)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        // append without overlap - just a write op
        assert_eq!(
            add(&ranges, 200, vec![0; 50]),
            vec![Op::Write(ChunkMeta::new(200..250, 0), vec![0; 50])]
        );

        // append with full overlap - just delete
        assert_eq!(
            add(&ranges, 100, vec![0; 150]),
            vec![
                Op::Write(ChunkMeta::new(100..250, 1), vec![0; 150]),
                Op::Delete(ChunkMeta::new(100..200, 0)),
            ]
        );

        // append with overlap - first rewrite, then delete
        assert_eq!(
            add(&ranges, 190, vec![0; 50]),
            vec![
                Op::Write(ChunkMeta::new(190..240, 1), vec![0; 50]),
                Op::Rewrite(ChunkMeta::new(100..190, 1)),
                Op::Delete(ChunkMeta::new(100..200, 0)),
            ]
        );

        // overwrite in the middle - rewrite and delete
        assert_eq!(
            add(&ranges, 50, vec![0; 100]),
            vec![
                Op::Write(ChunkMeta::new(50..150, 1), vec![0; 100]),
                Op::Rewrite(ChunkMeta::new(0..50, 1)),
                Op::Delete(ChunkMeta::new(0..100, 0)),
                Op::Rewrite(ChunkMeta::new(150..200, 1)),
                Op::Delete(ChunkMeta::new(100..200, 0)),
            ]
        );

        // overwrite in the middle - rewrite and delete
        assert_eq!(
            add(&ranges, 25, vec![0; 50]),
            vec![
                Op::Write(ChunkMeta::new(25..75, 1), vec![0; 50]),
                Op::Rewrite(ChunkMeta::new(0..25, 1)),
                Op::Rewrite(ChunkMeta::new(75..100, 1)),
                Op::Delete(ChunkMeta::new(0..100, 0)),
            ]
        );
    }

    #[test]
    fn truncate_ops() {
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(100..200, 0)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        // truncate noop
        assert_eq!(truncate(&ranges, 200), vec![]);

        // truncate noop
        assert_eq!(truncate(&ranges, 250), vec![]);

        // truncate delete
        assert_eq!(
            truncate(&ranges, 100),
            vec![Op::Delete(ChunkMeta::new(100..200, 0)),]
        );

        // truncate rewrite 1
        assert_eq!(
            truncate(&ranges, 190),
            vec![
                Op::Rewrite(ChunkMeta::new(100..190, 1)),
                Op::Delete(ChunkMeta::new(100..200, 0)),
            ]
        );

        // truncate rewrite 2
        assert_eq!(
            truncate(&ranges, 10),
            vec![
                Op::Rewrite(ChunkMeta::new(0..10, 1)),
                Op::Delete(ChunkMeta::new(0..100, 0)),
                Op::Delete(ChunkMeta::new(100..200, 0)),
            ]
        );
    }
    #[test]
    fn open_ops() {
        // straight overwrite, crash before delete
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(0..100, 1)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(open(&ranges), vec![Op::Delete(ChunkMeta::new(0..100, 0))]);

        // partial overwrite 1
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(50..150, 0)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            open(&ranges),
            vec![
                Op::Rewrite(ChunkMeta::new(0..50, 1)),
                Op::Delete(ChunkMeta::new(0..100, 0))
            ]
        );

        // partial overwrite 2
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(25..75, 1)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            open(&ranges),
            vec![
                Op::Rewrite(ChunkMeta::new(0..25, 2)),
                Op::Rewrite(ChunkMeta::new(75..100, 2)),
                Op::Delete(ChunkMeta::new(0..100, 0))
            ]
        );

        // recover from truncate 1
        let ranges = vec![ChunkMeta::new(1..100, 0), ChunkMeta::new(25..75, 1)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            open(&ranges),
            vec![
                Op::Delete(ChunkMeta::new(25..75, 1)),
                Op::Delete(ChunkMeta::new(1..100, 0)),
            ]
        );

        // recover from truncate 2
        let ranges = vec![ChunkMeta::new(0..100, 0), ChunkMeta::new(101..201, 0)]
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            open(&ranges),
            vec![Op::Delete(ChunkMeta::new(101..201, 0)),]
        );
    }
}
