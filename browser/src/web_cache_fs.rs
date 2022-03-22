use anyhow::Context;
use futures::{channel::mpsc, FutureExt, StreamExt};
use js_sys::{Array, ArrayBuffer, Uint8Array};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ops::Range,
    str::FromStr,
};
use url::Url;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::{CacheQueryOptions, Request, Response};

use crate::{
    js_async, overlaps, ranges, sync_fs::SyncFs, worker_scope, Command, JsError, SharedStr,
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
            async move {
                let manager = Self {
                    queue: rx,
                    dirs: Default::default(),
                };
                manager.run().await;
            }
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

    async fn open_file(&mut self, dir: SharedStr, file: SharedStr) -> anyhow::Result<()> {
        let (dir, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        if !files.contains_key(&file) {
            let wcf = WebCacheFile::new(dir.cache.clone(), file.clone()).await?;
            files.insert(file, wcf);
        }
        Ok(())
    }

    async fn append_file(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.append_length_prefixed(data).await
    }

    async fn read_file(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        offset: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.load_length_prefixed(offset).await
    }

    async fn delete_file(&mut self, dir: SharedStr, file_name: SharedStr) -> anyhow::Result<bool> {
        let (dir, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let res = dir.delete(&file_name).await?;
        files.remove(&file_name);
        Ok(res)
    }

    async fn delete_dir(&mut self, dir_name: SharedStr) -> anyhow::Result<bool> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let res: JsValue = js_async(caches.delete(&dir_name)).await?;
        Ok(res.as_bool().context("no bool")?)
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
                Command::ReadFile {
                    dir_name,
                    file_name,
                    offset,
                    cb,
                } => {
                    let _ = cb.send(self.read_file(dir_name, file_name, offset).await);
                }
                Command::AppendFile {
                    dir_name,
                    file_name,
                    data,
                    cb,
                } => {
                    let _ = cb.send(self.append_file(dir_name, file_name, data).await);
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
                Command::Shutdown => break,
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

    pub(crate) async fn open(&self, file_name: SharedStr) -> anyhow::Result<WebCacheFile> {
        WebCacheFile::new(self.cache.clone(), file_name).await
    }

    pub(crate) async fn delete(&self, file_name: &str) -> anyhow::Result<bool> {
        let res = js_async::<JsValue>(
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
        let mut chunks = Self::chunks(&cache, &file_name).await?;
        let to_delete = Self::retain_relevant(&mut chunks);
        Self::delete(&cache, &file_name, &to_delete).await?;
        Ok(Self {
            cache,
            file_name,
            chunks,
        })
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
                if chunk.offset == 0 {
                    // new level starts at offset 0, take
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

    async fn delete(
        cache: &web_sys::Cache,
        file_name: &str,
        to_delete: &[ChunkMeta],
    ) -> anyhow::Result<()> {
        for chunk in to_delete {
            let request = chunk.to_request_str(file_name);
            JsFuture::from(cache.delete_with_str(&request))
                .await
                .map_err(JsError::from)?;
        }
        Ok(())
    }

    async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<Vec<ChunkMeta>> {
        let keys = JsFuture::from(cache.keys()).await.map_err(JsError::from)?;
        let chunks = Array::from(&keys)
            .iter()
            .filter_map(|req| ChunkMeta::parse_and_filter(&Request::from(req).url(), name).ok())
            .filter_map(|x| x)
            .collect::<Vec<_>>();
        Ok(chunks)
    }

    fn prev(&self) -> ChunkMeta {
        self.chunks.last().cloned().unwrap_or_default()
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
        JsFuture::from(self.cache.put_with_str(&request, &response))
            .await
            .expect("insertion not possible");
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

    fn intersecting_chunks(&self, range: Range<u64>) -> impl Iterator<Item = &ChunkMeta> {
        self.chunks
            .iter()
            .filter(move |chunk| overlaps(chunk.range(), range.clone()))
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

    pub(crate) async fn load_length_prefixed(&self, start: u64) -> anyhow::Result<Vec<u8>> {
        // info!("load_length_prefixed index={}", start);
        // let t0 = js_sys::Date::now();
        let chunks = self
            .intersecting_chunks(start..start + 4)
            .collect::<Vec<_>>();
        let end = chunks.iter().map(|x| x.range().end).max().unwrap_or(start);
        let mut res = self.load_chunks(start..end, chunks).await?;
        let len = usize::try_from(u32::from_be_bytes(res[0..4].try_into().unwrap()))?;
        anyhow::ensure!(res.len() >= len + 4);
        res.truncate(len + 4);
        res.splice(0..4, []);
        // info!("load_length_prefixed {}", js_sys::Date::now() - t0);
        Ok(res)
    }

    pub(crate) async fn read(&self, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        let len_u64 = range.end.saturating_sub(range.start);
        let len: usize = len_u64.try_into()?;
        let mut res = vec![0u8; len];
        for chunk in self.intersecting_chunks(range.clone()) {
            let (sr, tr) = ranges(chunk.range(), range.clone())?;
            let data = self.load_range(chunk, sr).await?;
            data.copy_to(&mut res[tr]);
        }
        Ok(res)
    }

    async fn load(&self, chunk: &ChunkMeta) -> anyhow::Result<Vec<u8>> {
        let request: String = chunk.to_request_str(&self.file_name);
        let response = Response::from(
            JsFuture::from(self.cache.match_with_str(&request))
                .await
                .map_err(JsError::from)
                .context("no response")?,
        );
        let ab = ArrayBuffer::from(
            JsFuture::from(
                response
                    .array_buffer()
                    .map_err(JsError::from)
                    .context("response no array buffer")?,
            )
            .await
            .map_err(JsError::from)
            .context("response no array buffer")?,
        );
        Ok(Uint8Array::new(&ab).to_vec())
    }

    async fn load_range(
        &self,
        chunk: &ChunkMeta,
        range: Range<usize>,
    ) -> anyhow::Result<Uint8Array> {
        let request: String = chunk.to_request_str(&self.file_name);
        // let t0 = js_sys::Date::now();
        let response = Response::from(
            JsFuture::from(self.cache.match_with_str(&request))
                .await
                .map_err(JsError::from)
                .context("no response")?,
        );
        // info!("get response {}", js_sys::Date::now() - t0);
        const USE_BLOB: bool = true;
        let ab = if USE_BLOB {
            // let t0 = js_sys::Date::now();
            let blob = web_sys::Blob::from(
                JsFuture::from(
                    response
                        .blob()
                        .map_err(JsError::from)
                        .context("response no blob")?,
                )
                .await
                .map_err(JsError::from)
                .context("response no blob")?,
            );
            // info!("load blob {}", js_sys::Date::now() - t0);
            // let t0 = js_sys::Date::now();
            let slice = blob
                .slice_with_i32_and_i32(range.start.try_into()?, range.end.try_into()?)
                .map_err(JsError::from)?;
            // info!("slice blob {}", js_sys::Date::now() - t0);
            // let t0 = js_sys::Date::now();
            let res = ArrayBuffer::from(
                JsFuture::from(slice.array_buffer())
                    .await
                    .map_err(JsError::from)
                    .context("doh")?,
            );
            // info!("blob -> arraybuffer {}", js_sys::Date::now() - t0);
            res
        } else {
            let ab = ArrayBuffer::from(
                JsFuture::from(
                    response
                        .array_buffer()
                        .map_err(JsError::from)
                        .context("response no array buffer")?,
                )
                .await
                .map_err(JsError::from)
                .context("response no array buffer")?,
            );
            ab.slice_with_end(range.start.try_into()?, range.end.try_into()?)
        };
        // let t0 = js_sys::Date::now();
        let ua = Uint8Array::new(&ab);
        // info!("convert to uint8array {}", js_sys::Date::now() - t0);
        Ok(ua)
    }
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
