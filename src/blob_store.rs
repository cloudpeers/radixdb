use crate::{blob::BlobOwner, Blob};

use super::Hex;
use anyhow::Context;
use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

pub trait BlobStoreError {}

impl<T: From<anyhow::Error> + From<NoError>> BlobStoreError for T {}

pub trait BlobStore: Debug + Send + Sync {
    type Error: From<NoError> + From<anyhow::Error>;

    fn read(&self, id: u64) -> std::result::Result<Blob<u8>, Self::Error>;

    fn write(&self, data: &[u8]) -> std::result::Result<u64, Self::Error>;

    fn sync(&self) -> std::result::Result<(), Self::Error>;
}

pub type DynBlobStore = Box<dyn BlobStore<Error = anyhow::Error>>;

impl BlobStore for DynBlobStore {
    type Error = anyhow::Error;

    fn read(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        self.as_ref().read(id)
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<u64> {
        self.as_ref().write(data)
    }

    fn sync(&self) -> anyhow::Result<()> {
        self.as_ref().sync()
    }
}

#[derive(Default, Debug, Clone)]
pub struct NoStore;

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

pub fn unwrap_safe<T>(x: Result<T, NoError>) -> T {
    x.unwrap()
}

impl BlobStore for NoStore {
    type Error = NoError;

    fn read(&self, _: u64) -> std::result::Result<Blob<u8>, Self::Error> {
        panic!("no store");
    }

    fn write(&self, _: &[u8]) -> std::result::Result<u64, Self::Error> {
        panic!("no store");
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        panic!("no store");
    }
}

#[derive(Default, Debug, Clone)]
pub struct NoStoreDyn;

impl NoStoreDyn {
    pub fn new() -> DynBlobStore {
        Box::new(Self)
    }
}

impl BlobStore for NoStoreDyn {
    type Error = anyhow::Error;

    fn read(&self, _: u64) -> anyhow::Result<Blob<u8>> {
        anyhow::bail!("no store");
    }

    fn write(&self, _: &[u8]) -> anyhow::Result<u64> {
        anyhow::bail!("no store");
    }

    fn sync(&self) -> anyhow::Result<()> {
        anyhow::bail!("no store");
    }
}

#[derive(Default, Clone)]
pub struct MemStore {
    data: Arc<Mutex<BTreeMap<u64, Blob<u8>>>>,
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

    fn read(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        let data = self.data.lock();
        data.get(&id).cloned().context("value not found")
    }

    fn write(&self, sluce: &[u8]) -> anyhow::Result<u64> {
        let mut data = self.data.lock();
        let max = data.keys().next_back().cloned().unwrap_or(0);
        let id = max + 1;
        let blob = Blob::arc_from_byte_slice(sluce);
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
    recent: FnvHashMap<u64, Blob<u8>>,
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
    fn is_valid(&self, offset: usize) -> bool {
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
    fn bytes(&self, offset: usize) -> anyhow::Result<Blob<u8>> {
        Blob::<u8>::custom(self.0.clone(), offset)
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

    fn bytes(&self, offset: u64) -> anyhow::Result<Blob<u8>> {
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
        self.recent.insert(id, Blob::arc_from_byte_slice(data));
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

    fn read(&self, offset: u64) -> anyhow::Result<Blob<u8>> {
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
                    prop_assert!(offset + 4 + block.len() > TEST_SIZE || (offset % 8) != 0);
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
