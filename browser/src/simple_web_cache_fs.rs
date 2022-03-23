use anyhow::Context;
use futures::{channel::mpsc, FutureExt, StreamExt};
use js_sys::{Array, ArrayBuffer, Uint8Array};
use radixdb::Blob;
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ops::Range,
    str::FromStr,
};
use url::Url;
use wasm_bindgen::JsValue;
use wasm_futures_executor::ThreadPool;
use web_sys::{Cache, CacheQueryOptions, Request, Response};

use crate::{
    js_async, overlap, ranges, sync_fs::SyncFs, worker_scope, Command, JsError, SharedStr,
};

/// A virtual file system that uses the browser cache as storage
pub struct WebCacheFs {
    dirs: BTreeMap<SharedStr, (WebCacheDir, BTreeMap<SharedStr, WebCacheFile>)>,
    queue: mpsc::UnboundedReceiver<Command>,
}

impl WebCacheFs {
    /// create a new WebCacheFs on a separate thread, and wrap it in a SyncFs
    pub fn new(pool: ThreadPool) -> SyncFs {
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
    chunks: Vec<ChunkMeta>,
}

impl WebCacheFile {
    pub(crate) async fn new(cache: web_sys::Cache, file_name: SharedStr) -> anyhow::Result<Self> {
        let mut chunks = chunks(&cache, &file_name).await?;
        let to_delete = retain_relevant(&mut chunks);
        delete(&cache, &file_name, &to_delete).await?;
        Ok(Self {
            cache,
            file_name,
            chunks,
        })
    }

    fn prev(&self) -> ChunkMeta {
        self.chunks.last().cloned().unwrap_or_default()
    }

    pub(crate) async fn flush(&mut self) -> anyhow::Result<()> {
        // simple fs flushes after each op
        Ok(())
    }

    pub(crate) async fn append(&mut self, data: &mut Vec<u8>) -> anyhow::Result<u64> {
        let prev = self.prev();
        let chunk = ChunkMeta {
            level: prev.level,
            offset: prev.range().end,
            len: data.len().try_into()?,
        };
        let request = chunk.to_request_str(&self.file_name);
        let response = Response::new_with_opt_u8_array(Some(data)).map_err(JsError::from)?;
        let _: JsValue = js_async(self.cache.put_with_str(&request, &response)).await?;
        self.chunks.push(chunk);
        Ok(chunk.range().start)
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
            let data = self.load_range(chunk, sr).await?;
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
            let data = self.load_range(chunk, sr).await?;
            data.copy_to(&mut res[tr]);
        }
        Ok(res)
    }

    pub(crate) async fn write(&mut self, offset: u64, mut data: Vec<u8>) -> anyhow::Result<()> {
        let start = offset;
        let end = start + u64::try_from(data.len())?;
        let range = start..end;
        let current_end = self.prev().range().end;
        anyhow::ensure!(start <= current_end, "non consecutive write");
        anyhow::ensure!(end >= current_end, "write in the middle");
        let chunk = self.mk_chunk(range);
        write_chunk(&self.cache, &self.file_name, &chunk, &mut data).await?;
        self.chunks.push(chunk);
        let to_delete = retain_relevant(&mut self.chunks);
        delete(&self.cache, &self.file_name, &to_delete).await?;
        Ok(())
    }

    pub(crate) async fn length(&mut self) -> anyhow::Result<u64> {
        Ok(self.prev().range().end)
    }

    async fn load(&self, chunk: &ChunkMeta) -> anyhow::Result<Vec<u8>> {
        let request: String = chunk.to_request_str(&self.file_name);
        let response: Response = js_async(self.cache.match_with_str(&request)).await?;
        let ab: ArrayBuffer = js_async(response.array_buffer().map_err(JsError::from)?).await?;
        Ok(Uint8Array::new(&ab).to_vec())
    }

    async fn load_range(
        &self,
        chunk: &ChunkMeta,
        range: Range<usize>,
    ) -> anyhow::Result<Uint8Array> {
        let request: String = chunk.to_request_str(&self.file_name);
        // let t0 = js_sys::Date::now();
        let response: Response = js_async(self.cache.match_with_str(&request)).await?;
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

async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<Vec<ChunkMeta>> {
    let keys: JsValue = js_async(cache.keys()).await?;
    let chunks = Array::from(&keys)
        .iter()
        .filter_map(|req| ChunkMeta::parse_and_filter(&Request::from(req).url(), name).ok())
        .filter_map(|x| x)
        .collect::<Vec<_>>();
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
            "/{}?{:016x}-{:016x}-{:016x}",
            file_name, self.level, self.offset, self.len
        )
    }
}

impl ToString for ChunkMeta {
    fn to_string(&self) -> String {
        format!("{:016x}-{:016x}-{:016x}", self.level, self.offset, self.len)
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
