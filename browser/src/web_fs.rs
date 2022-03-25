//!
//! A web file system based on the browser file api, and the origin private file system.
//! 
//! https://web.dev/file-system-access/#accessing-the-origin-private-file-system
use anyhow::Context;
use futures::{channel::mpsc::{self, UnboundedReceiver}, FutureExt, StreamExt, Future};
use js_sys::{Array, ArrayBuffer, Uint8Array};
use log::info;
use radixdb::Blob;
use wasm_bindgen_futures::JsFuture;
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
pub struct WebFs {
    root: web_sys::Directory,
    dirs: BTreeMap<SharedStr, (WebDir, BTreeMap<SharedStr, WebFile>)>,
    queue: mpsc::UnboundedReceiver<Command>,
}

/// Get the origin private file system. This will only work on chrome based browsers (for now?)
/// 
/// It will hopefully return a good error if it fails.
pub async fn get_origin_private_file_system_root() -> anyhow::Result<web_sys::Directory> {
    let storage_manager = worker_scope().navigator().storage();
    let get_directory_fn = js_sys::Reflect::get(&storage_manager, &"getDirectory".into()).map_err(JsError::from)
        .context("unable to get the getDirectory fn on StorageManager")?;
    let get_directory_fn = js_sys::Function::from(get_directory_fn);
    let promise = js_sys::Reflect::apply(&get_directory_fn, &storage_manager, &js_sys::Array::new())
        .map_err(JsError::from).context("unable to call the getDirectory fn on StorageManager")?;
    let promise = js_sys::Promise::from(promise);    
    let root = JsFuture::from(promise).await
        .map_err(JsError::from)
        .context("calling getDirectory produced an error")?;
    let root = web_sys::Directory::from(root);
    Ok(root)
}

impl WebFs {
    /// create a new WebCacheFs on a separate thread, and wrap it in a SyncFs
    /// 
    /// `pool` a thread pool that is used to spawn 1 io handler thread
    /// `mk_dir` a function that gets called on the io handler thread to create a directory
    pub fn new<F, Fut>(pool: ThreadPool, mk_dir: F) -> anyhow::Result<SyncFs>
        where
            F: FnOnce() -> Fut + Send + 'static,
            Fut: Future<Output = anyhow::Result<web_sys::Directory>>,
    {
        let (tx, rx) = mpsc::unbounded();    
        pool.spawn_lazy(|| {
            async move {
                let root = mk_dir().await.unwrap();
                Self {
                    queue: rx,
                    dirs: Default::default(),
                    root,
                }
                .run().await
            }
            .boxed_local()
        });
        Ok(SyncFs::new(tx))
    }

    async fn open_dir(&mut self, dir_name: impl Into<SharedStr>) -> anyhow::Result<()> {
        let dir_name = dir_name.into();
        if !self.dirs.contains_key(&dir_name) {
            let dir = WebDir::new(&dir_name).await?;
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
pub struct WebDir {
    /// the cache which we abuse as a directory
    cache: web_sys::Cache,
}

impl WebDir {
    pub(crate) async fn new(dir_name: &str) -> anyhow::Result<Self> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let cache = js_async(caches.open(dir_name)).await?;
        Ok(Self { cache })
    }

    pub(crate) async fn open_file(&self, file_name: SharedStr) -> anyhow::Result<WebFile> {
        WebFile::new(self.cache.clone(), file_name).await
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
pub struct WebFile {
    /// the cache (basically the directory where the file lives in)
    file: web_sys::File,
}

impl WebFile {
    pub(crate) async fn new(file: web_sys::File) -> anyhow::Result<Self> {
        Ok(Self {
            file,
        })
    }

    pub(crate) async fn flush(&mut self) -> anyhow::Result<()> {
        // simple fs flushes after each op
        Ok(())
    }

    pub(crate) async fn append(&mut self, data: &mut Vec<u8>) -> anyhow::Result<u64> {
        todo!()
    }

    pub(crate) async fn append_length_prefixed(
        &mut self,
        mut data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        data.splice(0..0, u32::try_from(data.len()).unwrap().to_be_bytes());
        self.append(&mut data).await
    }

    pub(crate) async fn load_length_prefixed(&self, start: u64) -> anyhow::Result<Blob<u8>> {
        todo!()
    }

    pub(crate) async fn read(&self, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        todo!()
    }

    pub(crate) async fn write(&mut self, offset: u64, mut data: Vec<u8>) -> anyhow::Result<()> {
        todo!()
    }

    pub(crate) async fn length(&mut self) -> anyhow::Result<u64> {
        let size_f64 = self.file.size();
        let size_u64 = size_f64 as u64;
        anyhow::ensure!(size_u64 as f64 == size_f64);
        Ok(size_u64)
    }

    pub(crate) async fn truncate(&mut self, size: u64) -> anyhow::Result<()> {
        todo!()
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
    // todo: is there a way to do this without getting all keys?
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
