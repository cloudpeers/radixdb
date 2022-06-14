use crate::Hex;
use anyhow::Context;
use parking_lot::Mutex;
use std::{
    any::Any,
    borrow::Borrow,
    collections::BTreeMap,
    fmt::Debug,
    ops::{Bound, Deref, RangeBounds},
    sync::Arc,
};

/// A generic blob store with variable id size
pub trait BlobStore: Debug + Send + Sync + 'static {
    /// The error. Use NoError for a store that can never fail
    type Error: From<NoError> + From<anyhow::Error> + Debug;

    /// Read a blob with the given id. Since ids can be of arbitrary size, passed as a slice
    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error>;

    /// Write a blob, returning an id into a target vec `tgt`.
    ///
    /// If this returns an error, the tgt vec is guaranteed to be unmodified.
    fn write(&self, data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error>;

    /// Ensure all data is persisted
    fn sync(&self) -> std::result::Result<(), Self::Error>;

    /// True if the store needs deep detach. This is true for basically all stores except the special NoStore store
    fn needs_deep_detach(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Blob<'a> {
    data: &'a [u8],
    owner: Option<Arc<dyn Any>>,
}

unsafe impl<'a> Sync for Blob<'a> {}
unsafe impl<'a> Send for Blob<'a> {}

impl<'a> AsRef<[u8]> for Blob<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> Deref for Blob<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a> Borrow<[u8]> for Blob<'a> {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

pub type OwnedBlob = Blob<'static>;

impl<'a> Blob<'a> {
    pub fn empty() -> Self {
        Self::new(&[])
    }

    pub fn new(data: &'a [u8]) -> Self {
        Self { data, owner: None }
    }

    pub fn copy_from_slice(data: &[u8]) -> OwnedBlob {
        OwnedBlob::from_arc_vec(Arc::new(data.to_vec()))
    }

    pub fn from_arc_vec(arc: Arc<Vec<u8>>) -> OwnedBlob {
        let data: &[u8] = arc.as_ref();
        // extend the lifetime
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        OwnedBlob::owned_new(data, Some(arc))
    }

    pub fn to_owned(self) -> OwnedBlob {
        if self.owner.is_some() {
            OwnedBlob {
                data: unsafe { std::mem::transmute(self.data) },
                owner: self.owner,
            }
        } else {
            OwnedBlob::copy_from_slice(&self.data)
        }
    }

    /// When calling this with an owner, you promise that keeping the owner alive will keep the slice valid!
    pub fn owned_new(data: &'a [u8], owner: Option<Arc<dyn Any>>) -> Self {
        Self { data, owner }
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let len = self.len();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {:?} <= {:?}",
            begin,
            end,
        );
        assert!(
            end <= len,
            "range end out of bounds: {:?} <= {:?}",
            end,
            len,
        );

        if end == begin {
            return Self::empty();
        }

        Self {
            data: &self.data[begin..end],
            owner: self.owner.clone(),
        }
    }

    // copied from the Bytes crate
    pub fn slice_ref(&self, subset: &[u8]) -> Self {
        // Empty slice and empty Bytes may have their pointers reset
        // so explicitly allow empty slice to be a subslice of any slice.
        if subset.is_empty() {
            return Self::empty();
        }

        let bytes_p = self.as_ptr() as usize;
        let bytes_len = self.len();

        let sub_p = subset.as_ptr() as usize;
        let sub_len = subset.len();

        assert!(
            sub_p >= bytes_p,
            "subset pointer ({:p}) is smaller than self pointer ({:p})",
            sub_p as *const u8,
            bytes_p as *const u8,
        );
        assert!(
            sub_p + sub_len <= bytes_p + bytes_len,
            "subset is out of bounds: self = ({:p}, {}), subset = ({:p}, {})",
            bytes_p as *const u8,
            bytes_len,
            sub_p as *const u8,
            sub_len,
        );

        let sub_offset = sub_p - bytes_p;

        self.slice(sub_offset..(sub_offset + sub_len))
    }
}

impl From<Arc<Vec<u8>>> for OwnedBlob {
    fn from(v: Arc<Vec<u8>>) -> Self {
        let bytes: &[u8] = v.as_ref();
        // extend the lifetime
        // the vec will be unchanged and will be kept alive by the arc
        let bytes: &'static [u8] = unsafe { std::mem::transmute(bytes) };
        Self::owned_new(bytes, Some(v))
    }
}

/// Type for a dynamic blob store
///
/// Uses Arc so the dynamic reference can be cheaply cloned.
pub type DynBlobStore = Arc<dyn BlobStore<Error = anyhow::Error>>;

impl BlobStore for DynBlobStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        self.as_ref().read(id)
    }

    fn write(&self, data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        self.as_ref().write(data)
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        self.as_ref().sync()
    }

    fn needs_deep_detach(&self) -> bool {
        self.as_ref().needs_deep_detach()
    }
}

/// A special store that does nothing, to be used with detached trees that don't use a store
#[derive(Debug, Clone, Default)]
pub struct NoStore;

/// The implementation of NoStore will panic whenever it is used
impl BlobStore for NoStore {
    type Error = NoError;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        panic!()
    }

    fn write(&self, data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        panic!()
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        panic!()
    }

    fn needs_deep_detach(&self) -> bool {
        false
    }
}

/// An error type with zero inhabitants, similar to Infallible
///
/// When using this error, error handling code will not be generated.
#[derive(Debug)]
pub enum NoError {}

impl From<NoError> for anyhow::Error {
    fn from(_: NoError) -> Self {
        panic!()
    }
}

impl From<anyhow::Error> for NoError {
    fn from(_: anyhow::Error) -> Self {
        panic!()
    }
}

/// Unwrap an unfallible result
pub fn unwrap_safe<T>(x: Result<T, NoError>) -> T {
    x.unwrap()
}

/// A simple in memory store
#[derive(Default, Clone)]
pub struct MemStore {
    data: Arc<Mutex<BTreeMap<u64, Arc<Vec<u8>>>>>,
}

impl Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_map();
        let data = self.data.lock();
        for (id, v) in data.iter() {
            builder.entry(&id, &Hex::partial(v.as_ref(), 128));
        }
        builder.finish()
    }
}

impl BlobStore for MemStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        anyhow::ensure!(id.len() == 8);
        let id = u64::from_be_bytes(id.try_into().unwrap());
        let data = self.data.lock();
        data.get(&id)
            .map(|x| Blob::from_arc_vec(x.clone()))
            .context("value not found")
    }

    fn write(&self, slice: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        let mut data = self.data.lock();
        let max = data.keys().next_back().cloned().unwrap_or(0);
        let id = max + 1;
        let blob = Arc::new(slice.to_vec());
        data.insert(id, blob);
        Ok(id.to_be_bytes().to_vec())
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const TEST_SIZE: usize = 1024;

    fn large_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..TEST_SIZE - 4),
            1..10,
        )
    }

    fn small_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..TEST_SIZE / 10),
            1..100,
        )
    }

    fn test_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop_oneof![large_blocks(), small_blocks(),]
    }

    proptest! {}
}
