use futures::FutureExt;
use js_sys::{Date, Promise, JSON};
use log::{info, trace};
use parking_lot::Mutex;
use radixdb::{BlobStore, DynBlobStore, TreeNode};
use sqlite_vfs::{OpenOptions, VfsResult, VfsError};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
    io::{self, ErrorKind, Read, Seek, Write},
    ops::Range,
    sync::Arc, ffi::CStr,
};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::DedicatedWorkerGlobalScope;

mod sync_fs;
use sync_fs::Command;
pub use sync_fs::{SyncDir, SyncFile, SyncFs};
mod simple_web_cache_fs;
pub use simple_web_cache_fs::WebCacheFs as SimpleWebCacheFs;
mod web_fs;
pub use web_fs::WebFs as WebFs;
mod paging_file;
pub use paging_file::PagingFile;

use crate::{simple_web_cache_fs::WebCacheDir, web_fs::get_origin_private_file_system_root};

// macro_rules! console_log {
//     // Note that this is using the `log` function imported above during
//     // `bare_bones`
//     ($($t:tt)*) => (web_sys::console::log_1(&format_args!($($t)*).to_string().into()))
// }

/// A string that can be cheaply sent over thread boundaries
pub(crate) type SharedStr = Arc<str>;

/// execute a promise, handle errors, and convert the weird js result into an anyhow::Result
pub(crate) async fn js_async<T: From<JsValue>>(promise: Promise) -> anyhow::Result<T> {
    let res = JsFuture::from(promise).await.map_err(JsError::from)?;
    Ok(T::from(res))
}

/// given two ranges, returns the source and target range for associated slices
///
/// this function answers the question: given a slice `src_slice`, a range `src`,
/// a slice `tgt_slice` and a range `tgt`, what range of the source slice will be copied
/// into what range of the target slice.
///
/// it properly handles all u64 -> usize conversion overflows
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

/// true if two ranges overlap
fn overlap(a: Range<u64>, b: Range<u64>) -> bool {
    !((a.end <= b.start) || (b.end <= a.start))
}

/// The worker scope. This also works when not in a web worker
fn worker_scope() -> DedicatedWorkerGlobalScope {
    js_sys::global().unchecked_into::<DedicatedWorkerGlobalScope>()
}

/// A serialized JsValue error that can be sent over thread boundaries.
#[derive(Debug)]
pub(crate) enum JsError {
    Stringified(String),
    Error(String),
}

impl Display for JsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl std::error::Error for JsError {}

impl From<JsValue> for JsError {
    fn from(value: JsValue) -> Self {
        match JSON::stringify(&value) {
            Ok(text) => JsError::Stringified(text.into()),
            Err(cause) => JsError::Error(format!("{:?}", cause)),
        }
    }
}

impl TryFrom<JsError> for JsValue {
    type Error = JsValue;

    fn try_from(value: JsError) -> Result<Self, Self::Error> {
        match value {
            JsError::Stringified(txt) => match JSON::parse(&txt) {
                Ok(value) => Ok(value),
                Err(cause) => Err(cause),
            },
            JsError::Error(text) => Err(text.into()),
        }
    }
}

/// Convert from an anyhow::Error back to a JsValue for use with wasm-bindgen
fn err_to_jsvalue(value: anyhow::Error) -> JsValue {
    match value.downcast::<JsError>() {
        Ok(inner) => JsValue::try_from(inner).unwrap_or_else(|e| e),
        Err(value) => value.to_string().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_test() {
        assert!(overlap(0..1, 0..1));
        assert!(!overlap(0..1, 1..2));
        assert!(!overlap(1..2, 0..1));
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

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let _ = console_log::init_with_level(log::Level::Info);
    ::console_error_panic_hook::set_once();
    pool_test().await.map_err(err_to_jsvalue)?;
    Ok("done".into())
}

async fn pool_test() -> anyhow::Result<()> {
    let pool = ThreadPool::new(2).await.map_err(JsError::from)?;
    let pool2 = pool.clone();
    // the outer spawn is necessary so we don't block on the main thread, which is not allowed
    pool.spawn_lazy(move || {
        async move {
            // info!("starting cache_bench");
            // cache_bench().await.unwrap();
            info!("starting sqlite_test");
            sqlite_test(pool2).unwrap();
            // info!("starting cache_test");
            // cache_test().await.unwrap();
            // info!("starting cache_test_sync");
            // cache_test_sync(pool2).unwrap();
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
    let mut file = dir.open_file("file1".into()).await?;
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

async fn cache_bench() -> anyhow::Result<()> {
    let dir = WebCacheDir::new("bench").await?;
    let mut file = dir.open_file("file1".into()).await?;
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

fn now() -> f64 {
    Date::now() / 1000.0
}

fn do_test(mut store: DynBlobStore) -> anyhow::Result<()> {
    let elems = (0..2000000).map(|i| {
        if i % 100000 == 0 {
            info!("{}", i);
        }
        (
            i.to_string().as_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
        )
    });
    let t0 = now();
    info!("building tree");
    let mut tree: TreeNode = elems.collect();
    info!("unattached tree {:?} {} s", tree, now() - t0);
    info!("traversing unattached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("attaching tree...");
    let t0 = now();
    tree.attach(&mut store)?;
    store.flush()?;
    info!("attached tree {:?} {} s", tree, now() - t0);
    info!("traversing attached tree values...");
    let t0 = now();
    let mut n = 0;
    for item in tree.try_values(&store) {
        if item.is_err() {
            info!("{:?}", item);
        }
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("traversing attached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("detaching tree...");
    let t0 = now();
    tree.detach(&store, true)?;
    info!("detached tree {:?} {} s", tree, now() - t0);
    info!("traversing unattached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    Ok(())
}

fn cache_test_sync(pool: ThreadPool) -> anyhow::Result<()> {
    let fs = SimpleWebCacheFs::new(pool);
    fs.delete_dir("sync")?;
    let dir = fs.open_dir("sync")?;
    dir.delete_file("test")?;
    let file = dir.open_file("test")?;
    let file = PagingFile::new(file, 1024 * 1024)?;
    let store: DynBlobStore = Box::new(file);
    do_test(store)
}

#[derive(Debug)]
pub struct SqliteVfsFile {
    inner: SyncFile,
}

impl SqliteVfsFile {
    fn new(file: SyncFile) -> Self {
        Self {
            inner: file,
        }
    }
}

impl sqlite_vfs::File for SqliteVfsFile {

    fn read_exact(&mut self, start: u64, buf: &mut [u8]) -> VfsResult<usize> {
        info!("read_exact {:?}, {}", self, buf.len());
        let current_len = self.inner.length().map_err(anyhow_to_vfs)?;
        let end = current_len.min(start + buf.len() as u64);
        let range = start..end;
        let len = end.saturating_sub(start);
        let data = self.inner.read(range).map_err(anyhow_to_vfs);
        let data = data?;
        let len_usize = len as usize;
        buf[0..len_usize].copy_from_slice(&data[..len_usize]);
        Ok(len_usize)
    }

    fn write_all(&mut self, start: u64, buf: &[u8]) -> VfsResult<usize> {
        info!("write_all {:?} {} {}", self, start, buf.len());
        let res = self.inner
            .write(start, buf.to_vec())
            .map_err(anyhow_to_vfs)?;
        Ok(buf.len())
    }

    fn file_size(&self) -> VfsResult<u64> {
        info!("file_size {:?}", self);
        self.inner.length().map_err(anyhow_to_vfs)
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        info!("truncate {:?} {}", self, size);
        self.inner.truncate(size).map_err(anyhow_to_vfs)
    }

    fn flush(&mut self) -> VfsResult<()> {
        self.inner.flush().map_err(anyhow_to_vfs)
    }
}

fn anyhow_to_vfs(error: anyhow::Error) -> VfsError {
    sqlite_vfs::SQLITE_IOERR
}

impl sqlite_vfs::Vfs for SyncDir {
    type File = SqliteVfsFile;

    fn open(&self, path: &CStr, opts: OpenOptions) -> VfsResult<Self::File> {
        let path = path.to_string_lossy();
        info!("open {:?} {} {:?}", self, path, opts);
        let inner = self.open_file(path).map_err(anyhow_to_vfs)?;
        Ok(SqliteVfsFile::new(inner))
    }

    fn delete(&self, path: &CStr) -> VfsResult<()> {
        let path = path.to_string_lossy();
        info!("delete {:?} {}", self, path);
        self.delete_file(path).map_err(anyhow_to_vfs)?;
        Ok(())
    }

    fn exists(&self, path: &CStr) -> VfsResult<bool> {
        let path = path.to_string_lossy();
        info!("exists {:?} {}", self, path);
        let res = self.file_exists(path).map_err(anyhow_to_vfs)?;
        Ok(res)
    }
}

fn sqlite_test(pool: ThreadPool) -> anyhow::Result<()> {
    use rusqlite::{Connection, OpenFlags};
    // let fs = SimpleWebCacheFs::new(pool);
    let fs = WebFs::new(pool, || get_origin_private_file_system_root())?;
    fs.delete_dir("sqlite")?;
    let dir = fs.open_dir("sqlite")?;

    sqlite_vfs::register("test", dir).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )?;

    conn.execute_batch(
        r#"
        PRAGMA page_size = 32768;
        PRAGMA journal_mode = MEMORY;
        "#,
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
        [],
    )?;

    for i in 0..10 {
        conn.execute("INSERT INTO vals (val) VALUES ('test')", [])?;
    }

    let n: i64 = conn.query_row("SELECT COUNT(*) FROM vals", [], |row| row.get(0))?;

    info!("Count: {}", n);
    conn.cache_flush()?;
    drop(conn);
    Ok(())
}
