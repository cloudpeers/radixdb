use futures::FutureExt;
use js_sys::{Promise, JSON};
use log::info;
use radixdb::{DynBlobStore, TreeNode};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
    ops::Range,
    sync::Arc,
};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::DedicatedWorkerGlobalScope;

mod sync_fs;
use sync_fs::Command;
pub use sync_fs::{SyncDir, SyncFile, SyncFs};
mod web_cache_fs;
pub use web_cache_fs::WebCacheFs;

use crate::web_cache_fs::WebCacheDir;

// macro_rules! console_log {
//     // Note that this is using the `log` function imported above during
//     // `bare_bones`
//     ($($t:tt)*) => (web_sys::console::log_1(&format_args!($($t)*).to_string().into()))
// }

/// A string that can be cheaply sent over thread boundaries
pub(crate) type SharedStr = Arc<str>;

/// execute a promise, handle errors, and convert to to a result
pub(crate) async fn js_async<T: From<JsValue>>(promise: Promise) -> anyhow::Result<T> {
    let res = JsFuture::from(promise).await.map_err(JsError::from)?;
    Ok(T::from(res))
}

/// given two ranges, returns the source and target range for associated slices
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
fn overlaps(a: Range<u64>, b: Range<u64>) -> bool {
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

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let _ = console_log::init_with_level(log::Level::Info);
    ::console_error_panic_hook::set_once();
    pool_test().await.map_err(err_to_jsvalue)?;
    Ok(5.into())
}

async fn pool_test() -> anyhow::Result<()> {
    let pool = ThreadPool::new(2).await.map_err(JsError::from)?;
    let pool2 = pool.clone();
    // the outer spawn is necessary so we don't block on the main thread, which is not allowed
    pool.spawn_lazy(move || {
        async move {
            // info!("starting cache_bench");
            // cache_bench().await.unwrap();
            info!("starting cache_test");
            cache_test().await.unwrap();
            info!("starting cache_test_sync");
            cache_test_sync(pool2).unwrap();
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
    let fs = WebCacheFs::new(pool);
    fs.delete_dir("sync")?;
    let dir = fs.open_dir("sync")?;
    dir.delete_file("test")?;
    let file = dir.open_file("test")?;
    let elems = (0..100000)
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
    info!("unattached tree {:?}", tree);
    info!("attaching tree...");
    tree.attach(&mut store)?;
    info!("attached tree {:?}", tree);
    info!("detaching tree...");
    tree.detach(&store, true)?;
    info!("detached tree {:?}", tree);
    Ok(())
}
