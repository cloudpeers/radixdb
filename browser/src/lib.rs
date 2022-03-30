use futures::FutureExt;
use js_sys::{Date, Promise, JSON};
use log::{info, trace};
use parking_lot::Mutex;
use radixdb::{BlobStore, DynBlobStore, TreeNode};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ffi::CStr,
    fmt::{Debug, Display},
    io::{self, ErrorKind, Read, Seek, Write},
    ops::Range,
    sync::Arc,
};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::DedicatedWorkerGlobalScope;

mod sync_fs;
use sync_fs::Command;
pub use sync_fs::{SyncDir, SyncFile, SyncFs};
mod web_cache_fs;
pub use web_cache_fs::WebCacheFs as SimpleWebCacheFs;
mod paging_file;
pub use paging_file::PagingFile;
mod vfs;
pub use vfs::*;
mod browser;

use crate::web_cache_fs::WebCacheDir;

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

/// true if a contains b
fn contains(a: Range<u64>, b: Range<u64>) -> bool {
    a.start <= b.start && a.end >= b.end
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
