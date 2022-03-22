use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use futures::{channel::mpsc, executor::block_on};
use futures::{FutureExt, StreamExt};
use js_sys::Promise;
use js_sys::{Array, ArrayBuffer, Uint8Array};
use log::*;
use radixdb::DynBlobStore;
use radixdb::TreeNode;
use radixdb::{Blob, BlobStore};
use url::Url;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::DedicatedWorkerGlobalScope;
use web_sys::{CacheQueryOptions, Request, Response};

// macro_rules! console_log {
//     // Note that this is using the `log` function imported above during
//     // `bare_bones`
//     ($($t:tt)*) => (web_sys::console::log_1(&format_args!($($t)*).to_string().into()))
// }

type SharedStr = Arc<str>;

#[derive(Debug, Clone)]
struct WebCacheDir {
    /// the cache which we abuse as a directory
    cache: web_sys::Cache,
}

async fn js_async<T: From<JsValue>>(promise: Promise) -> anyhow::Result<T> {
    JsFuture::from(promise)
        .await
        .map_err(js_to_anyhow)
        .map(|x| T::from(x))
}

impl WebCacheDir {
    pub async fn new(dir_name: &str) -> anyhow::Result<Self> {
        let caches = worker_scope().caches().map_err(js_to_anyhow)?;
        let cache = js_async(caches.open(dir_name)).await?;
        Ok(Self { cache })
    }

    pub async fn open(&self, file_name: SharedStr) -> anyhow::Result<WebCacheFile> {
        WebCacheFile::new(self.cache.clone(), file_name).await
    }

    pub async fn create(&self, file_name: SharedStr) -> anyhow::Result<WebCacheFile> {
        self.delete(&file_name).await?;
        self.open(file_name).await
    }

    pub async fn delete(&self, file_name: &str) -> anyhow::Result<bool> {
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
struct WebCacheFile {
    /// the cache (basically the directory where the file lives in)
    cache: web_sys::Cache,
    /// the file name
    file_name: SharedStr,
    /// all the chunks that we have, sorted
    chunks: Vec<ChunkMeta>,
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
        info!("parse_and_filter {}", request);
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

impl WebCacheFile {
    async fn new(cache: web_sys::Cache, file_name: SharedStr) -> anyhow::Result<Self> {
        let mut chunks = Self::chunks(&cache, &file_name).await?;
        info!("chunks {:#?}", chunks);
        let to_delete = Self::retain_relevant(&mut chunks);
        info!("to_delete {:#?}", to_delete);
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
                .map_err(js_to_anyhow)?;
        }
        Ok(())
    }

    async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<Vec<ChunkMeta>> {
        let keys = JsFuture::from(cache.keys()).await.map_err(js_to_anyhow)?;
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

    async fn append(&mut self, data: &mut Vec<u8>) -> anyhow::Result<u64> {
        let prev = self.prev();
        let chunk = ChunkMeta {
            level: prev.level,
            offset: prev.range().end,
            len: data.len().try_into()?,
        };
        let request = chunk.to_request_str(&self.file_name);
        let response = Response::new_with_opt_u8_array(Some(data)).map_err(js_to_anyhow)?;
        JsFuture::from(self.cache.put_with_str(&request, &response))
            .await
            .expect("insertion not possible");
        self.chunks.push(chunk);
        Ok(chunk.range().start)
    }

    async fn append_length_prefixed(&mut self, mut data: Vec<u8>) -> anyhow::Result<u64> {
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
        let t0 = js_sys::Date::now();
        let len = range.end.saturating_sub(range.start).try_into()?;
        let mut res = vec![0u8; len];
        for chunk in chunks.into_iter() {
            let (sr, tr) = ranges(chunk.range(), range.clone())?;
            let t0 = js_sys::Date::now();
            let data = self.load_range(chunk, sr).await?;
            info!("load_range {}", js_sys::Date::now() - t0);
            data.copy_to(&mut res[tr]);
        }
        info!("load_chunks {}", js_sys::Date::now() - t0);
        Ok(res)
    }

    async fn load_length_prefixed(&self, start: u64) -> anyhow::Result<Vec<u8>> {
        info!("load_length_prefixed index={}", start);
        let t0 = js_sys::Date::now();
        let chunks = self
            .intersecting_chunks(start..start + 4)
            .collect::<Vec<_>>();
        let end = chunks.iter().map(|x| x.range().end).max().unwrap_or(start);
        let mut res = self.load_chunks(start..end, chunks).await?;
        let len = usize::try_from(u32::from_be_bytes(res[0..4].try_into().unwrap()))?;
        anyhow::ensure!(res.len() >= len + 4);
        res.truncate(len + 4);
        res.splice(0..4, []);
        info!("load_length_prefixed {}", js_sys::Date::now() - t0);
        Ok(res)
    }

    async fn read(&self, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
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
                .map_err(js_to_anyhow)
                .context("no response")?,
        );
        let ab = ArrayBuffer::from(
            JsFuture::from(
                response
                    .array_buffer()
                    .map_err(js_to_anyhow)
                    .context("response no array buffer")?,
            )
            .await
            .map_err(js_to_anyhow)
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

        let t0 = js_sys::Date::now();
        let response = Response::from(
            JsFuture::from(self.cache.match_with_str(&request))
                .await
                .map_err(js_to_anyhow)
                .context("no response")?,
        );
        info!("get response {}", js_sys::Date::now() - t0);
        const USE_BLOB: bool = true;
        let ab = if USE_BLOB {
            let t0 = js_sys::Date::now();
            let blob = web_sys::Blob::from(
                JsFuture::from(
                    response
                        .blob()
                        .map_err(js_to_anyhow)
                        .context("response no blob")?,
                )
                .await
                .map_err(js_to_anyhow)
                .context("response no blob")?,
            );
            info!("load blob {}", js_sys::Date::now() - t0);
            let t0 = js_sys::Date::now();
            let slice = blob
                .slice_with_i32_and_i32(range.start.try_into()?, range.end.try_into()?)
                .map_err(js_to_anyhow)?;
            info!("slice blob {}", js_sys::Date::now() - t0);
            let t0 = js_sys::Date::now();
            let res = ArrayBuffer::from(
                JsFuture::from(slice.array_buffer())
                    .await
                    .map_err(js_to_anyhow)
                    .context("doh")?,
            );
            info!("blob -> arraybuffer {}", js_sys::Date::now() - t0);
            res
        } else {
            let ab = ArrayBuffer::from(
                JsFuture::from(
                    response
                        .array_buffer()
                        .map_err(js_to_anyhow)
                        .context("response no array buffer")?,
                )
                .await
                .map_err(js_to_anyhow)
                .context("response no array buffer")?,
            );
            ab.slice_with_end(range.start.try_into()?, range.end.try_into()?)
        };
        let t0 = js_sys::Date::now();
        let ua = Uint8Array::new(&ab);
        info!("convert to uint8array {}", js_sys::Date::now() - t0);
        Ok(ua)
    }
}

#[derive(Debug)]
enum Command {
    OpenDir {
        dir_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    DeleteDir {
        dir_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<bool>>,
    },
    OpenFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    DeleteFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<bool>>,
    },
    AppendFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        data: Vec<u8>,
        cb: oneshot::Sender<anyhow::Result<u64>>,
    },
    ReadFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        offset: u64,
        cb: oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
}

struct IoManager {
    dirs: BTreeMap<SharedStr, (WebCacheDir, BTreeMap<SharedStr, WebCacheFile>)>,
    queue: mpsc::UnboundedReceiver<Command>,
}

impl IoManager {
    fn new(queue: mpsc::UnboundedReceiver<Command>) -> anyhow::Result<Self> {
        Ok(Self {
            queue,
            dirs: Default::default(),
        })
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
        let (dir, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.append_length_prefixed(data).await
    }

    async fn read_file(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        offset: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let (dir, files) = self.dirs.get_mut(&dir).context("dir not open")?;
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
        let caches = worker_scope().caches().map_err(js_to_anyhow)?;
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
            }
        }
    }
}

#[derive(Debug)]
pub struct SyncFs {
    tx: UnboundedSender<Command>,
}

impl SyncFs {
    fn new(pool: ThreadPool) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::unbounded();
        pool.spawn_lazy(|| {
            async move {
                let manager = IoManager::new(rx).unwrap();
                manager.run().await;
            }
            .boxed_local()
        });
        Ok(Self { tx })
    }
    fn open_dir(&self, dir_name: impl Into<SharedStr>) -> anyhow::Result<SyncDir> {
        let dir_name = dir_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::OpenDir {
            dir_name: dir_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(SyncDir {
            dir_name,
            tx: self.tx.clone(),
        })
    }
    fn delete_dir(&self, dir_name: impl Into<SharedStr>) -> anyhow::Result<SyncDir> {
        let dir_name = dir_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::DeleteDir {
            dir_name: dir_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(SyncDir {
            dir_name,
            tx: self.tx.clone(),
        })
    }
}

#[derive(Debug)]
pub struct SyncDir {
    tx: UnboundedSender<Command>,
    dir_name: SharedStr,
}

impl SyncDir {
    fn open_file(&self, file_name: impl Into<SharedStr>) -> anyhow::Result<SyncFile> {
        let file_name = file_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::OpenFile {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(SyncFile {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            tx: self.tx.clone(),
        })
    }
}

#[derive(Debug)]
pub struct SyncFile {
    dir_name: SharedStr,
    file_name: SharedStr,
    tx: UnboundedSender<Command>,
}

impl SyncFile {
    fn new(dir_name: SharedStr, file_name: SharedStr, tx: UnboundedSender<Command>) -> Self {
        Self {
            dir_name,
            file_name,
            tx,
        }
    }
}

impl radixdb::BlobStore for SyncFile {
    fn bytes(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::ReadFile {
            offset: id,
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            cb: tx,
        })?;
        let data = block_on(rx)??;
        Ok(Blob::from_slice(&data))
    }

    fn append(&self, data: &[u8]) -> anyhow::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::AppendFile {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            data: data.to_vec(),
            cb: tx,
        })?;
        let offset = block_on(rx)??;
        Ok(offset)
    }
}

fn ranges(src: Range<u64>, tgt: Range<u64>) -> anyhow::Result<(Range<usize>, Range<usize>)> {
    let src_len = src.end.saturating_sub(src.start);
    let sr = i64::try_from(tgt.start.saturating_sub(src.start).min(src_len))?
        ..i64::try_from(tgt.end.saturating_sub(src.start).min(src_len))?;
    let t = i64::try_from(src.start)? - i64::try_from(tgt.start)?;
    let tr = sr.start + t..sr.end + t;
    Ok((
        sr.start.try_into()?..sr.end.try_into()?,
        tr.start.max(0).try_into()?..tr.end.max(0).try_into()?,
    ))
}

fn overlaps(a: Range<u64>, b: Range<u64>) -> bool {
    !((a.end <= b.start) || (b.end <= a.start))
}

// #[wasm_bindgen(start)]
// pub fn main() {
//     let _ = console_log::init_with_level(log::Level::Debug);
//     ::console_error_panic_hook::set_once();
// }

fn worker_scope() -> DedicatedWorkerGlobalScope {
    js_sys::global().unchecked_into::<DedicatedWorkerGlobalScope>()
}

fn worker_name() -> String {
    worker_scope().name().as_str().to_string()
}

fn js_to_anyhow(js: JsValue) -> anyhow::Error {
    anyhow::anyhow!("Damn {:?}", js)
}

fn err_to_jsvalue(value: impl ToString) -> JsValue {
    value.to_string().into()
}

// #[wasm_bindgen]
// pub async fn start() -> Result<JsValue, JsValue> {
//     let pool = ThreadPool::new(2).await?;
//     let pool2 = pool.clone();
//     pool.spawn_lazy(|| {
//         async {
//             let cache: web_sys::Cache =
//                 JsFuture::from(worker_scope().caches().unwrap().open("cache"))
//                     .await
//                     .unwrap()
//                     .into();
//         }
//         .boxed_local()
//     });
//     Ok(0.into())
// }

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let _ = console_log::init_with_level(log::Level::Info);
    ::console_error_panic_hook::set_once();
    info!("");
    // cache_test().await.map_err(err_to_jsvalue)?;
    pool_test().await.map_err(err_to_jsvalue)?;
    Ok(5.into())
}

async fn pool_test() -> anyhow::Result<()> {
    let pool = ThreadPool::new(2).await.map_err(js_to_anyhow)?;
    let pool2 = pool.clone();
    // the outer spawn is necessary so we don't block on the main thread, which is not allowed
    pool.spawn_lazy(move || {
        async move {
            // info!("starting cache_bench");
            // cache_bench().await.unwrap();
            info!("starting cache_test");
            cache_test().await.unwrap();
            info!("starting cache_test_sync");
            cache_test_sync(pool2.clone()).unwrap();
        }
        .boxed_local()
    });
    Ok(())
}

async fn cache_test() -> anyhow::Result<()> {
    info!("{}", worker_scope().location().origin());
    let dir = WebCacheDir::new("test").await?;
    let deleted = dir.delete("file1").await?;
    info!("deleted {}", deleted);
    let mut file = dir.open("file1".into()).await?;
    for i in 0..5u8 {
        let mut data = vec![i; 100];
        file.append(&mut data).await?;
    }
    let data = file.read(10..240).await?;
    info!("{:?}", dir);
    info!("{:?}", file);
    info!("{}", hex::encode(data));
    Ok(())
}

fn time<T>(text: &str, f: impl FnOnce() -> T) -> T {
    let t0 = js_sys::Date::now();
    let res = f();
    let dt = js_sys::Date::now() - t0;
    info!("{} {}", text, dt);
    res
}

async fn cache_bench() -> anyhow::Result<()> {
    let dir = WebCacheDir::new("bench").await?;
    let mut file = dir.open("file1".into()).await?;
    let mut data = vec![0u8; 1024 * 1024 * 4];
    data[0..4].copy_from_slice(&100u32.to_be_bytes());
    let t0 = js_sys::Date::now();
    file.append(&mut data).await?;
    let dt = js_sys::Date::now() - t0;
    info!("write {}", dt);
    let t0 = js_sys::Date::now();
    let blob = file.load_length_prefixed(0).await?;
    let dt = js_sys::Date::now() - t0;
    info!("read {} {}", dt, blob.len());
    Ok(())
}

fn cache_test_sync(pool: ThreadPool) -> anyhow::Result<()> {
    let fs = SyncFs::new(pool)?;
    let dir = fs.open_dir("sync")?;
    let file = dir.open_file("test")?;
    let elems = (0..1000)
        .map(|i| {
            (
                i.to_string().as_bytes().to_vec(),
                i.to_string().as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let mut store: DynBlobStore = Box::new(file);
    let mut tree: TreeNode = elems
        .iter()
        .map(|(k, v)| (k.as_ref(), v.as_ref()))
        .collect();
    info!("{:?}", tree);
    tree.attach(&mut store)?;
    info!("{:?}", tree);
    tree.detach(&store, true)?;
    info!("{:?}", tree);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_test() {
        assert!(overlaps(0..1, 0..1));
        assert!(!overlaps(0..1, 1..2));
        assert!(!overlaps(1..2, 0..1));
    }

    #[test]
    fn range_test() -> anyhow::Result<()> {
        // chunk fits
        let (s, t) = ranges(0..1, 0..1)?;
        assert!(s == (0..1) && t == (0..1));
        // chunk fits with room to spare
        let (s, t) = ranges(10..20, 10..110)?;
        assert!(s == (0..10) && t == (0..10));
        // chunk too large
        let (s, t) = ranges(10..110, 10..20)?;
        assert!(s == (0..10) && t == (0..10));
        // chunk end truncated
        let (s, t) = ranges(10..20, 10..15)?;
        assert!(s == (0..5) && t == (0..5));
        // chunk start truncated
        let (s, t) = ranges(10..20, 15..20)?;
        assert!(s == (5..10) && t == (0..5));
        // no overlap 1
        let (s, t) = ranges(10..20, 100..150)?;
        assert!(s == (10..10) && t == (0..0));
        // no overlap 2
        let (s, t) = ranges(100..150, 10..15)?;
        assert!(s == (0..0) && t == (90..90));
        Ok(())
    }
}
