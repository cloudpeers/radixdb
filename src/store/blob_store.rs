use crate::{
    store::{Blob, BlobOwner},
    Hex,
};
use anyhow::Context;
use fnv::FnvHashMap;
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
pub trait BlobStore2: Debug + Send + Sync + 'static {
    /// The error. Use NoError for a store that can never fail
    type Error: From<NoError> + From<anyhow::Error> + Debug;

    /// Read a blob with the given id. Since ids can be of arbitrary size, passed as a slice
    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error>;

    /// Write a blob, returning an id into a target vec `tgt`.
    ///
    /// If this returns an error, the tgt vec is guaranteed to be unmodified.
    fn write(&self, data: &[u8], tgt: &mut Vec<u8>) -> std::result::Result<(), Self::Error>;

    /// Ensure all data is persisted
    fn sync(&self) -> std::result::Result<(), Self::Error>;

    /// True if the store needs deep detach. This is true for basically all stores except the special NoStore store
    fn needs_deep_detach(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Blob2<'a> {
    data: &'a [u8],
    owner: Option<Arc<dyn Any>>,
}

impl<'a> AsRef<[u8]> for Blob2<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> Deref for Blob2<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a> Borrow<[u8]> for Blob2<'a> {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

pub type OwnedBlob = Blob2<'static>;

impl<'a> Blob2<'a> {
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
        // the vec will be unchanged and will be kept alive by the arc
        let bytes: &'static [u8] = unsafe { std::mem::transmute(bytes) };
        Self::owned_new(bytes, Some(v))
    }
}

/// Type for a dynamic blob store
///
/// Uses Arc so the dynamic reference can be cheaply cloned.
pub type DynBlobStore2 = Arc<dyn BlobStore2<Error = anyhow::Error>>;

impl BlobStore2 for DynBlobStore2 {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        self.as_ref().read(id)
    }

    fn write(&self, data: &[u8], tgt: &mut Vec<u8>) -> std::result::Result<(), Self::Error> {
        self.as_ref().write(data, tgt)
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        self.as_ref().sync()
    }

    fn needs_deep_detach(&self) -> bool {
        self.as_ref().needs_deep_detach()
    }
}

/// A generic blob store
pub trait BlobStore: Debug + Send + Sync + 'static {
    type Error: From<NoError> + From<anyhow::Error> + Debug;

    /// Read a blob with the given id / extra value
    fn read(&self, id: u64) -> std::result::Result<Blob, Self::Error>;

    /// Write a blob, returning an id
    fn write(&self, data: &[u8]) -> std::result::Result<u64, Self::Error>;

    /// Ensure all data is persisted
    fn sync(&self) -> std::result::Result<(), Self::Error>;

    /// True if the store needs deep detach. This is true for basically all stores except the special NoStore store
    fn needs_deep_detach(&self) -> bool {
        true
    }
}

/// Type for a dynamic blob store
///
/// Uses Arc so the dynamic reference can be cheaply cloned.
pub type DynBlobStore = Arc<dyn BlobStore<Error = anyhow::Error>>;

impl BlobStore for DynBlobStore {
    type Error = anyhow::Error;

    fn read(&self, id: u64) -> anyhow::Result<Blob> {
        self.as_ref().read(id)
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<u64> {
        self.as_ref().write(data)
    }

    fn sync(&self) -> anyhow::Result<()> {
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

    fn read(&self, _: u64) -> std::result::Result<Blob, Self::Error> {
        panic!("no store");
    }

    fn write(&self, _: &[u8]) -> std::result::Result<u64, Self::Error> {
        panic!("no store");
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        panic!("no store");
    }

    fn needs_deep_detach(&self) -> bool {
        false
    }
}

/// The implementation of NoStore will panic whenever it is used
impl BlobStore2 for NoStore {
    type Error = NoError;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        panic!()
    }

    fn write(&self, data: &[u8], tgt: &mut Vec<u8>) -> std::result::Result<(), Self::Error> {
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
    data: Arc<Mutex<BTreeMap<u64, Blob>>>,
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

    fn read(&self, id: u64) -> anyhow::Result<Blob> {
        let data = self.data.lock();
        data.get(&id).cloned().context("value not found")
    }

    fn write(&self, sluce: &[u8]) -> anyhow::Result<u64> {
        let mut data = self.data.lock();
        let max = data.keys().next_back().cloned().unwrap_or(0);
        let id = max + 1;
        let blob = Blob::from_slice(sluce);
        data.insert(id, blob);
        Ok(id)
    }

    fn sync(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Inner<const SIZE: usize> {
    pages: FnvHashMap<u64, Page<SIZE>>,
    recent: FnvHashMap<u64, Blob>,
    offset: u64,
}

#[derive(Default, Clone)]
pub struct PagedMemStore<const SIZE: usize>(Arc<Mutex<Inner<SIZE>>>);

const ALIGN: usize = 8;

#[repr(C, align(8))]
struct PageInner<const SIZE: usize> {
    data: [u8; SIZE],
}

impl<const SIZE: usize> Debug for PageInner<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("PageInner<{}>", SIZE))
            .field("data", &Hex::partial(&self.data, 128))
            .finish()
    }
}

impl<const SIZE: usize> PageInner<SIZE> {
    fn new() -> Self {
        Self { data: [0u8; SIZE] }
    }
}

impl<const SIZE: usize> BlobOwner for Arc<PageInner<SIZE>> {
    fn get_slice(&self, offset: usize) -> &[u8] {
        let data = &self.as_ref().data;
        let base = offset + 4;
        let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        &data[base..base + length]
    }
    fn validate(&self, offset: usize) -> bool {
        let data = &self.as_ref().data;
        if offset + 4 <= data.len() {
            let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset + 4 + length <= data.len()
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct Page<const SIZE: usize>(Arc<dyn BlobOwner>);

struct PageBuilder<const SIZE: usize> {
    data: Arc<PageInner<SIZE>>,
}

impl<const SIZE: usize> PageBuilder<SIZE> {
    fn new() -> Self {
        Self {
            data: Arc::new(PageInner::new()),
        }
    }

    /// try to write the bytes at the given offset
    fn write(&mut self, offset: usize, data: &[u8]) -> anyhow::Result<()> {
        let len = data.len();
        anyhow::ensure!(
            offset + data.len() + 4 < SIZE,
            "slice extends beyond the end of the page"
        );
        anyhow::ensure!(offset & 7 == 0, "offsets must be 8 byte aligned");
        let inner = Arc::get_mut(&mut self.data).context("non exclusive access")?;
        inner.data[offset..offset + 4].copy_from_slice(&(len as u32).to_be_bytes());
        inner.data[offset + 4..offset + len + 4].copy_from_slice(data);
        Ok(())
    }

    fn build(self) -> Page<SIZE> {
        Page(Arc::new(self.data))
    }
}

impl<const SIZE: usize> Page<SIZE> {
    /// try to get the bytes at the given offset
    fn bytes(&self, offset: usize) -> anyhow::Result<Blob> {
        Blob::new(self.0.clone(), offset)
    }
}

impl<const SIZE: usize> Debug for Inner<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedMemStore")
            .field("pages", &self.pages.len())
            .field("recent", &self.recent.len())
            .field("offset", &self.offset)
            .finish()
    }
}

impl<const SIZE: usize> Debug for PagedMemStore<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.lock(), f)
    }
}

impl<const SIZE: usize> Inner<SIZE> {
    pub fn new() -> Self {
        assert!(SIZE % ALIGN == 0);
        Self::default()
    }
    const fn page(offset: u64) -> u64 {
        offset / (SIZE as u64)
    }
    const fn offset_within_page(offset: u64) -> u64 {
        offset % (SIZE as u64)
    }
    const fn offset_of_page(page: u64) -> u64 {
        page * (SIZE as u64)
    }
    const fn current_page(&self) -> u64 {
        Self::page(self.offset)
    }
    /// write everything in recent into a page, and purge it from recent
    fn write_current_page(&mut self) {
        let current_page = self.current_page();
        let mut page = PageBuilder::new();
        for (offset, blob) in &self.recent {
            if Self::page(*offset) == current_page {
                page.write(Self::offset_within_page(*offset) as usize, blob)
                    .unwrap();
            }
        }
        self.pages.insert(self.current_page(), page.build());
        self.recent
            .retain(|offset, _| Self::page(*offset) != current_page);
    }

    fn bytes(&self, offset: u64) -> anyhow::Result<Blob> {
        let page = Self::page(offset);
        let page_offset = Self::offset_within_page(offset);
        // first try pages, then recent
        if let Some(page) = self.pages.get(&page) {
            page.bytes(page_offset as usize)
        } else if let Some(blob) = self.recent.get(&offset) {
            Ok(blob.clone())
        } else {
            anyhow::bail!("page not found {}", page);
        }
    }

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        anyhow::ensure!(data.len() < SIZE - 4, "block too large for this store");
        // len of the data when stored, including length prefix
        let len = data.len() as u64 + 4;
        // new end
        let mut end = self.offset.checked_add(len).context("out of offsets")?;
        let end_page = Self::page(end);
        // check if we cross a page boundary
        if end_page != self.current_page() {
            self.write_current_page();
            self.offset = Self::offset_of_page(end_page);
            end = self.offset.checked_add(len).context("out of offsets")?;
        }
        let id = self.offset;
        self.recent.insert(id, Blob::from_slice(data));
        // make sure the new offset is also aligned
        while (end % (ALIGN as u64)) != 0 {
            end += 1;
        }
        // for a real impl, we would now atomically write to the end of the file
        self.offset = end;
        Ok(id)
    }
}

impl<const SIZE: usize> PagedMemStore<SIZE> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner::new())))
    }
}

impl<const SIZE: usize> BlobStore for PagedMemStore<SIZE> {
    type Error = anyhow::Error;

    fn read(&self, offset: u64) -> anyhow::Result<Blob> {
        self.0.lock().bytes(offset)
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<u64> {
        self.0.lock().append(data)
    }

    fn sync(&self) -> anyhow::Result<()> {
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

    proptest! {

        #[test]
        fn page_test(block in proptest::collection::vec(any::<u8>(), 0..TEST_SIZE + 100), offset in 0usize..TEST_SIZE + 100) {
            let mut page = PageBuilder::<TEST_SIZE>::new();
            match page.write(offset, &block) {
                Ok(_) => {
                    let page = page.build();
                    if let Ok(blob) = page.bytes(offset) {
                        prop_assert_eq!(blob.as_ref(), &block);
                    } else {
                        prop_assert!(false);
                    }
                },
                Err(_) => {
                    prop_assert!(offset + 4 + block.len() >= TEST_SIZE || (offset % 8) != 0);
                }
            }
        }

        #[test]
        fn paged_store_test(blocks in test_blocks()) {
            let store = PagedMemStore::<TEST_SIZE>::new();
            let res =
                blocks
                    .into_iter()
                    .map(|block| store.write(block.as_ref())
                        .map(|offset| (offset, block))).collect::<anyhow::Result<Vec<_>>>().unwrap();
            for (offset, block) in res.iter() {
                let actual = store.read(*offset).unwrap();
                let expected: &[u8] = &block;
                prop_assert_eq!(actual.as_ref(), expected);
            }
            println!("{:?}", store);
            for page in store.0.lock().pages.values() {
                println!("{:?}", page);
            }
        }
    }
}
