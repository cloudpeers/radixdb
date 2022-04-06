use fnv::FnvHashMap;
use memmap::{Mmap, MmapOptions};
use parking_lot::Mutex;

use crate::{
    blob::{Blob, BlobOwner},
    blob_store::BlobStore,
    Hex,
};
use anyhow::Context;
use std::{
    fmt::Debug,
    fs::{self, File},
    io::{Seek, Write},
    path::Path,
    sync::Arc,
};

#[derive(Clone)]
pub struct PagedFileStore<const SIZE: usize>(Arc<Mutex<Inner<SIZE>>>);

impl<const SIZE: usize> Debug for PagedFileStore<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.lock();
        f.debug_struct("PagedFileStore")
            .field("pages", &inner.pages.len())
            .field("recent", &inner.recent.len())
            .field("size", &(inner.pages.len() * SIZE))
            .finish()
    }
}

struct Inner<const SIZE: usize> {
    file: File,
    pages: FnvHashMap<u64, Page<SIZE>>,
    recent: FnvHashMap<u64, Blob<u8>>,
}

const ALIGN: usize = 8;

#[repr(C, align(8))]
struct PageInner<const SIZE: usize> {
    mmap: Mmap,
}

impl<const SIZE: usize> Debug for PageInner<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("PageInner<{}>", SIZE))
            .field("mmap", &&self.mmap)
            .finish()
    }
}

impl<const SIZE: usize> PageInner<SIZE> {
    fn new(mmap: Mmap) -> Self {
        assert!(mmap.len() == SIZE);
        Self { mmap }
    }
}

impl<const SIZE: usize> BlobOwner for Arc<PageInner<SIZE>> {
    fn get_slice(&self, offset: usize) -> &[u8] {
        let data = self.mmap.as_ref();
        let base = offset + 4;
        let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        &data[base..base + length]
    }
    fn is_valid(&self, offset: usize) -> bool {
        let data = self.mmap.as_ref();
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

impl<const SIZE: usize> Page<SIZE> {
    fn new(mmap: Mmap) -> Self {
        assert!(mmap.len() == SIZE);
        Self(Arc::new(Arc::new(PageInner::<SIZE>::new(mmap))))
    }

    /// try to get the bytes at the given offset
    fn bytes(&self, offset: usize) -> anyhow::Result<Blob<u8>> {
        Blob::<u8>::custom(self.0.clone(), offset)
    }
}

impl<const SIZE: usize> Debug for Inner<SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedFileStore")
            .field("file", &self.file)
            .field("pages", &self.pages.len())
            .field("recent", &self.recent.len())
            .finish()
    }
}

impl<const SIZE: usize> Inner<SIZE> {
    pub fn new(file: File) -> anyhow::Result<Self> {
        assert!(SIZE % ALIGN == 0);
        Ok(Self {
            file,
            pages: Default::default(),
            recent: Default::default(),
        })
    }
    const fn page(offset: u64) -> u64 {
        offset / (SIZE as u64)
    }
    const fn offset_within_page(offset: u64) -> usize {
        (offset % (SIZE as u64)) as usize
    }
    const fn offset_of_page(page: u64) -> u64 {
        page * (SIZE as u64)
    }
    fn close_page(&mut self, current_page: u64) -> anyhow::Result<()> {
        // println!("close_page page={} offset={}", current_page, self.file.stream_position()?);
        let start = Self::offset_of_page(current_page);
        self.pad_to(start + (SIZE as u64))?;
        self.file.flush()?;
        let mmap = unsafe { MmapOptions::new().offset(start).len(SIZE).map(&self.file)? };
        // println!("{}", Hex::new(mmap.as_ref()));
        self.pages.insert(current_page, Page::new(mmap));
        self.recent
            .retain(|offset, _| Self::page(*offset) != current_page);
        Ok(())
    }

    fn pad_to(&mut self, offset: u64) -> anyhow::Result<()> {
        let padding = [0u8; 1024];
        loop {
            let pos = self.file.stream_position()?;
            if pos < offset {
                let n = (offset - pos).min(1024);
                self.file.write(&padding[..n as usize])?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn bytes(&self, offset: u64) -> anyhow::Result<Blob<u8>> {
        let page = Self::page(offset);
        let page_offset = Self::offset_within_page(offset);
        // first try pages, then recent
        if let Some(page) = self.pages.get(&page) {
            page.bytes(page_offset)
        } else if let Some(blob) = self.recent.get(&offset) {
            Ok(blob.clone())
        } else {
            anyhow::bail!("page not found {}", page);
        }
    }

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        anyhow::ensure!(data.len() <= SIZE - 8, "block too large for this store");
        // len of the data when stored, including length prefix
        let len = data.len() as u64 + 4;
        let offset = self.file.stream_position()?;
        // new end
        let end = offset + len;
        let current_page = Self::page(offset);
        let end_page = Self::page(end);
        // check if we cross a page boundary
        if end_page != current_page {
            self.close_page(current_page)?;
        }
        // make sure the content (not the size) is 8 byte aligned
        // todo: do this as a single write
        while (self.file.stream_position()? + 4) % 8 != 0 {
            self.file.write(&[0u8])?;
        }
        let id = self.file.stream_position()?;
        self.file.write_all(&(data.len() as u32).to_be_bytes())?;
        self.file.write_all(data)?;
        self.file.flush()?;
        self.recent.insert(id, Blob::arc_from_byte_slice(data));
        Ok(id)
    }
}

fn align(offset: u64) -> u64 {
    let mut res = offset;
    while (res % (ALIGN as u64)) != 0 {
        res += 1;
    }
    res
}

impl<const SIZE: usize> PagedFileStore<SIZE> {
    pub fn new(file: File) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(Inner::new(file)?))))
    }
}

impl<const SIZE: usize> BlobStore for PagedFileStore<SIZE> {
    type Error = anyhow::Error;

    fn read(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        self.0.lock().bytes(id)
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
    use std::{
        path::PathBuf,
        time::{Instant, SystemTime},
    };

    use crate::{DynBlobStore, TreeNode};

    use super::*;
    use log::info;
    use proptest::prelude::*;
    use tempfile::tempdir;
    use thousands::Separable;

    const TEST_SIZE: usize = 1024;

    fn large_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..TEST_SIZE - 8),
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

    fn ok<T>(f: impl Fn() -> anyhow::Result<T>) -> T {
        let res = f();
        res.ok().unwrap()
    }

    fn mk_block<const S: usize>(i: u64) -> [u8; S] {
        let mut data = [0u8; S];
        data[0..8].copy_from_slice(&i.to_be_bytes());
        data
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
        let t0 = Instant::now();
        info!("building tree");
        let mut tree: TreeNode = elems.collect();
        info!(
            "unattached tree {:?} {} s",
            tree,
            t0.elapsed().as_secs_f64()
        );
        info!("traversing unattached tree...");
        let t0 = Instant::now();
        let mut n = 0;
        for _ in tree.iter(&store)? {
            n += 1;
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
        info!("attaching tree...");
        let t0 = Instant::now();
        tree.attach(&mut store)?;
        store.sync()?;
        info!("attached tree {:?} {} s", tree, t0.elapsed().as_secs_f32());
        info!("traversing attached tree values...");
        let t0 = Instant::now();
        let mut n = 0;
        for item in tree.values(&store) {
            if item.is_err() {
                info!("{:?}", item);
            }
            n += 1;
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
        info!("traversing attached tree...");
        let t0 = Instant::now();
        let mut n = 0;
        for _ in tree.iter(&store)? {
            n += 1;
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
        info!("detaching tree...");
        let t0 = Instant::now();
        tree.detach(&store)?;
        info!("detached tree {:?} {} s", tree, t0.elapsed().as_secs_f32());
        info!("traversing unattached tree...");
        let t0 = Instant::now();
        let mut n = 0;
        for _ in tree.iter(&store)? {
            n += 1;
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
        Ok(())
    }

    fn init_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::max())
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    #[ignore = "too large"]
    fn browser_compare() -> anyhow::Result<()> {
        init_logger();
        let dir = tempdir()?;
        let path = dir.path().join("large2.rdb");
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let db = PagedFileStore::<1048576>::new(file).unwrap();
        let store: DynBlobStore = Box::new(db);
        do_test(store)
    }

    #[test]
    #[ignore = "too large"]
    fn paged_file_store_test_large() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("large.rdb");
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        println!("writing all of {:?}", path);
        let t = Instant::now();
        let db = PagedFileStore::<1048576>::new(file).unwrap();
        const BLOCK_SIZE: usize = 6666;
        const BLOCK_COUNT: u64 = 1000000;
        const TOTAL_SIZE: u64 = (BLOCK_SIZE as u64) * BLOCK_COUNT;
        let offsets = (0u64..BLOCK_COUNT)
            .map(|i| {
                let data = mk_block::<BLOCK_SIZE>(i);
                db.write(&data)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let dt = t.elapsed().as_secs_f64();
        println!(
            "done with {:?}, {}s, {}b/s",
            path,
            dt,
            ((TOTAL_SIZE as f64) / dt)
                .floor()
                .separate_with_underscores()
        );
        println!("reading all of {:?}", path);
        let t = Instant::now();
        for (i, offset) in offsets.into_iter().enumerate() {
            let expected = mk_block::<BLOCK_SIZE>(i as u64);
            let actual = db.read(offset)?;
            assert_eq!(&expected[..], actual.as_ref());
        }
        let dt = t.elapsed().as_secs_f64();
        println!(
            "done with {:?}, {}s, {}b/s",
            path,
            dt,
            ((TOTAL_SIZE as f64) / dt)
                .floor()
                .separate_with_underscores()
        );
        println!("total size {}", (BLOCK_SIZE as u64) * BLOCK_COUNT);
        Ok(())
    }

    proptest! {

        #[test]
        fn paged_file_store_test(blocks in test_blocks()) {
            let file = tempfile::tempfile().unwrap();
            let mut store = Inner::<TEST_SIZE>::new(file).unwrap();
            let res =
                blocks
                    .into_iter()
                    .map(|block| store.append(block.as_ref())
                        .map(|offset| (offset, block))).collect::<anyhow::Result<Vec<_>>>().unwrap();
            for (offset, block) in res.iter() {
                let actual = store.bytes(*offset).unwrap();
                let expected: &[u8] = &block;
                prop_assert_eq!(actual.as_ref(), expected);
            }
            println!("{:?}", store);
            for page in store.pages.values() {
                println!("{:?}", page);
            }
            println!();
        }
    }
}
