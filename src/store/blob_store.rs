use std::{
    any::Any,
    borrow::Borrow,
    cmp::Ordering,
    fmt::Debug,
    hash::Hash,
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

/// A blob that can be cheaply sliced
///
/// Implemented as a byte slice with an optional owner to keep the byte slice alive.
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

/// A blob that is self-contained and therefore has a static lifetime
///
/// An owned blob can be created either from a slice with an owner, or an actually static slice.
pub(crate) type OwnedBlob = Blob<'static>;

impl<'a> Hash for Blob<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl<'a, 'b> PartialEq<Blob<'b>> for Blob<'a> {
    fn eq(&self, other: &Blob<'b>) -> bool {
        self.data == other.data
    }
}

impl<'a> Eq for Blob<'a> {}

impl<'a, 'b> PartialOrd<Blob<'b>> for Blob<'a> {
    fn partial_cmp(&self, other: &Blob<'b>) -> Option<Ordering> {
        self.data.partial_cmp(other.data)
    }
}

impl<'a> Ord for Blob<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(other.data)
    }
}

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
            OwnedBlob::copy_from_slice(self.data)
        }
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
impl<const N: usize> From<Arc<[u8; N]>> for OwnedBlob {
    fn from(v: Arc<[u8; N]>) -> Self {
        let bytes: &[u8] = v.as_ref();
        // extend the lifetime
        // the array will be unchanged and will be kept alive by the arc
        let bytes: &'static [u8] = unsafe { std::mem::transmute(bytes) };
        Self::owned_new(bytes, Some(v))
    }
}

impl OwnedBlob {
    /// When calling this with an owner, you promise that keeping the owner alive will keep the slice valid!
    pub fn owned_new(data: &'static [u8], owner: Option<Arc<dyn Any>>) -> OwnedBlob {
        Self { data, owner }
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
pub struct Detached;

/// The implementation of NoStore will panic whenever it is used
impl BlobStore for Detached {
    type Error = NoError;

    fn read(&self, _id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        panic!()
    }

    fn write(&self, _data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
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

/// Extension trait that adds unwrap_safe for unwrapping results safely when the error type is uninhabited

pub trait UnwrapSafeExt<T> {
    /// Safe unwrap - guaranteed not to panic
    fn unwrap_safe(self) -> T;
}

impl<T> UnwrapSafeExt<T> for Result<T, NoError> {
    fn unwrap_safe(self) -> T {
        self.unwrap()
    }
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]
    use proptest::prelude::*;
    use std::any::Any;
    use std::sync::Arc;
    use tempfile::tempfile;

    use crate::store::blob_store::OwnedBlob;

    const TEST_SIZE: usize = 1024;

    unsafe fn custom_new(slice: &[u8], owner: Arc<dyn Any>) -> OwnedBlob {
        let slice: &'static [u8] = std::mem::transmute(slice);
        OwnedBlob::owned_new(slice, Some(owner))
    }

    #[test]
    fn zero_copy_mmap() -> anyhow::Result<()> {
        use memmap2::MmapOptions;
        use std::{io::Write, sync::Arc};
        // create a large file
        let mut large_file = tempfile().unwrap();
        large_file.write_all(&[0u8; 1024 * 1024])?;
        // map it and wrap the MMap in an arc
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&large_file).unwrap() });
        // create a bytes that points ot a part of the large file, without allocation
        // the Bytes instance keeps the mmap alive as long as needed
        let slice: &'static [u8] = unsafe { std::mem::transmute(&mmap[10..10000]) };
        let _bytes = OwnedBlob::owned_new(slice, Some(mmap.clone()));
        Ok(())
    }

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
