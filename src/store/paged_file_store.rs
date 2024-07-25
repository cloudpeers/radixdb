use fnv::FnvHashMap;
use memmap2::{Mmap, MmapOptions};
use parking_lot::Mutex;

use std::{
    fmt::Debug,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use super::{blob_store::OwnedBlob, BlobStore};

/// A blob store backed by a file that is divided into pages of size `SIZE`
#[derive(Clone)]
pub struct PagedFileStore(Arc<Mutex<Inner>>);

impl Debug for PagedFileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.lock();
        f.debug_struct("PagedFileStore")
            .field("pages", &inner.pages.len())
            .field("page_size", &inner.page_size)
            .field("recent", &inner.recent.len())
            .field("size", &((inner.pages.len() as u64) * inner.page_size))
            .finish()
    }
}

struct Inner {
    file: File,
    page_size: u64,
    pages: FnvHashMap<u64, Page>,
    recent: FnvHashMap<u64, OwnedBlob>,
    last_id: u64,
}

const ALIGN: usize = 8;
const HEADER_SIZE: u64 = 4096;

struct PageInner {
    page_size: usize,
    mmap: Mmap,
}

impl Debug for PageInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageInner")
            .field("page_size", &self.page_size)
            .field("mmap", &&self.mmap)
            .finish()
    }
}

impl PageInner {
    fn new(mmap: Mmap, page_size: usize) -> Self {
        assert!(mmap.len() == page_size);
        Self { mmap, page_size }
    }
}

#[derive(Debug, Clone)]
struct Page(Arc<PageInner>);

impl Page {
    fn new(mmap: Mmap, page_size: usize) -> Self {
        assert!(mmap.len() == page_size);
        Self(Arc::new(PageInner::new(mmap, page_size)))
    }

    /// try to get the bytes at the given offset
    fn bytes(&self, offset: usize) -> anyhow::Result<OwnedBlob> {
        let data = self.0.mmap.as_ref();
        anyhow::ensure!(offset >= 4);
        let length = u32::from_be_bytes(data[offset - 4..offset].try_into().unwrap()) as usize;
        anyhow::ensure!(offset >= length + 4);
        let slice: &[u8] = &data[offset - 4 - length..offset - 4];
        let slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        Ok(OwnedBlob::owned_new(slice, Some(self.0.clone())))
    }
}

impl Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedFileStore")
            .field("file", &self.file)
            .field("page_size", &self.page_size)
            .field("pages", &self.pages.len())
            .field("recent", &self.recent.len())
            .finish()
    }
}

fn page(offset: u64, page_size: u64) -> u64 {
    offset / page_size
}

fn offset_within_page(offset: u64, page_size: u64) -> usize {
    (offset % page_size) as usize
}

fn offset_of_page(page: u64, page_size: u64) -> u64 {
    page * page_size + HEADER_SIZE
}

fn read_size(file: &mut File) -> anyhow::Result<u64> {
    let mut buf = [0u8; 8];
    file.seek(SeekFrom::Start(16))?;
    file.read_exact(&mut buf)?;
    file.seek(SeekFrom::End(0))?;
    Ok(u64::from_be_bytes(buf))
}

fn write_size(file: &mut File, size: u64) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(16))?;
    file.write_all(&size.to_be_bytes())?;
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

impl Inner {
    pub fn new(mut file: File, page_size: u64) -> anyhow::Result<Self> {
        anyhow::ensure!((page_size as usize) % ALIGN == 0);
        let end = file.seek(std::io::SeekFrom::End(0))?;
        if end == 0 {
            // write header
            file.set_len(HEADER_SIZE)?;
            file.seek(std::io::SeekFrom::End(0))?;
        } else if end < HEADER_SIZE {
            // something went seriously wrong
            anyhow::bail!("Incomplete header!");
        }
        let size = read_size(&mut file)?;
        file.set_len(size + HEADER_SIZE)?;
        file.seek(std::io::SeekFrom::End(0))?;
        Ok(Self {
            file,
            page_size,
            pages: Default::default(),
            recent: Default::default(),
            last_id: size,
        })
    }

    fn load_page(&mut self, page: u64) -> anyhow::Result<()> {
        let start = offset_of_page(page, self.page_size);
        let mmap = unsafe {
            MmapOptions::new()
                .offset(start)
                .len(self.page_size as usize)
                .map(&self.file)?
        };
        self.pages
            .insert(page, Page::new(mmap, self.page_size as usize));
        Ok(())
    }

    fn load_recent(&mut self, id: u64) -> anyhow::Result<()> {
        anyhow::ensure!(id >= 4);
        self.file.seek(SeekFrom::Start(id + HEADER_SIZE - 4))?;
        let mut size = [0u8; 4];
        self.file.read_exact(&mut size)?;
        let size = u32::from_be_bytes(size) as u64;
        anyhow::ensure!(id >= 4 + size);
        self.file
            .seek(SeekFrom::Start(id + HEADER_SIZE - 4 - size))?;
        let mut buf = vec![0u8; size as usize];
        self.file.read_exact(&mut buf)?;
        self.file.seek(SeekFrom::End(0))?;
        self.recent
            .insert(id, OwnedBlob::from_arc_vec(Arc::new(buf)));
        Ok(())
    }

    fn close_page(&mut self, current_page: u64) -> anyhow::Result<()> {
        // println!("close_page page={} offset={}", current_page, self.file.stream_position()?);
        let start = offset_of_page(current_page, self.page_size);
        self.pad_to(start + (self.page_size as u64))?;
        self.commit()?;
        self.load_page(current_page)?;
        self.recent
            .retain(|offset, _| page(*offset, self.page_size) != current_page);
        Ok(())
    }

    fn pad_to(&mut self, offset: u64) -> anyhow::Result<()> {
        let padding = [0u8; 1024];
        loop {
            let pos = self.file.stream_position()?;
            if pos < offset {
                let n = (offset - pos).min(1024);
                self.file.write_all(&padding[..n as usize])?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn bytes(&mut self, offset: u64) -> anyhow::Result<OwnedBlob> {
        let last_page = page(self.last_id, self.page_size);
        let page = page(offset - 1, self.page_size);
        let page_offset = offset_within_page(offset, self.page_size);
        if page < last_page {
            if !self.pages.contains_key(&page) {
                self.load_page(page)?;
            }
            let page = self.pages.get(&page).expect("page must exist");
            page.bytes(page_offset)
        } else {
            if !self.recent.contains_key(&offset) {
                self.load_recent(offset)?;
            }
            let blob = self.recent.get(&offset).expect("blob must exist");
            Ok(blob.clone())
        }
    }

    fn commit(&mut self) -> anyhow::Result<u64> {
        let id = self.file.seek(SeekFrom::End(0))? - HEADER_SIZE;
        write_size(&mut self.file, id)?;
        self.file.flush()?;
        Ok(id)
    }

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        anyhow::ensure!(
            data.len() <= (self.page_size as usize) - 8,
            "block too large for this store"
        );
        // len of the data when stored, including length prefix
        let len = data.len() as u64 + 4;
        let position = self.file.seek(SeekFrom::End(0))?;
        if position < HEADER_SIZE {
            panic!();
        }
        let offset = position - HEADER_SIZE;
        // new end
        let end = offset + len;
        let current_page = page(offset, self.page_size);
        let end_page = page(end, self.page_size);
        // check if we cross a page boundary
        if end_page != current_page {
            self.close_page(current_page)?;
        }
        self.file.write_all(data)?;
        self.file.write_all(&(data.len() as u32).to_be_bytes())?;
        let id = self.commit()?;
        self.last_id = id;
        self.recent.insert(id, OwnedBlob::copy_from_slice(data));
        if id % self.page_size < 4 {
            assert!(id % 1024 > 4);
        }
        Ok(id)
    }
}

impl PagedFileStore {
    pub fn new(file: File, page_size: u64) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(Inner::new(file, page_size)?))))
    }

    pub fn last_id(&self) -> Option<[u8; 8]> {
        let id = self.0.lock().last_id;
        if id == 0 {
            None
        } else {
            Some(id.to_be_bytes())
        }
    }
}

impl BlobStore for PagedFileStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> anyhow::Result<OwnedBlob> {
        let offset = u64::from_be_bytes(id.try_into().unwrap());
        self.0.lock().bytes(offset)
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        let id = self.0.lock().append(data)?;
        Ok(id.to_be_bytes().to_vec())
    }

    fn sync(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Instant};

    use crate::store::DynBlobStore;

    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;
    use thousands::Separable;

    const TEST_SIZE: u64 = 1024;

    fn large_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..(TEST_SIZE as usize) - 8),
            1..10,
        )
    }

    fn small_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..(TEST_SIZE as usize) / 10),
            1..100,
        )
    }

    fn test_blocks() -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop_oneof![large_blocks(), small_blocks(),]
    }

    fn mk_block<const S: usize>(i: u64) -> [u8; S] {
        let mut data = [0u8; S];
        data[0..8].copy_from_slice(&i.to_be_bytes());
        data
    }

    // fn do_test(store: DynBlobStore2) -> anyhow::Result<()> {
    //     let elems = (0..2000000u64).map(|i| {
    //         if i % 100000 == 0 {
    //             info!("{}", i);
    //         }
    //         (
    //             i.to_string().as_bytes().to_vec(),
    //             i.to_string().as_bytes().to_vec(),
    //         )
    //     });
    //     let t0 = Instant::now();
    //     info!("building tree");
    //     let tree: VSRadixTree = elems.collect();
    //     info!(
    //         "unattached tree {:?} {} s",
    //         tree,
    //         t0.elapsed().as_secs_f64()
    //     );
    //     info!("traversing unattached tree...");
    //     let t0 = Instant::now();
    //     let mut n = 0;
    //     for _ in tree.iter() {
    //         n += 1;
    //     }
    //     info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    //     info!("attaching tree...");
    //     let t0 = Instant::now();
    //     // let tree = tree.attached(store.clone())?;
    //     store.sync()?;
    //     info!("attached tree {:?} {} s", tree, t0.elapsed().as_secs_f32());
    //     info!("traversing attached tree values...");
    //     let t0 = Instant::now();
    //     let mut n = 0;
    //     for item in tree.try_values() {
    //         if item.is_err() {
    //             info!("{:?}", item);
    //         }
    //         n += 1;
    //     }
    //     info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    //     info!("traversing attached tree...");
    //     let t0 = Instant::now();
    //     let mut n = 0;
    //     for _ in tree.try_iter() {
    //         n += 1;
    //     }
    //     info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    //     info!("detaching tree...");
    //     let t0 = Instant::now();
    //     // let tree = tree.detached()?;
    //     info!("detached tree {:?} {} s", tree, t0.elapsed().as_secs_f32());
    //     info!("traversing unattached tree...");
    //     let t0 = Instant::now();
    //     let mut n = 0;
    //     for _ in tree.iter() {
    //         n += 1;
    //     }
    //     info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    //     Ok(())
    // }

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
        let db = PagedFileStore::new(file, 1048576).unwrap();
        let _store: DynBlobStore = Arc::new(db);
        // do_test(store)
        todo!()
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
        let db = PagedFileStore::new(file, 1048576).unwrap();
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
            let actual = db.read(&offset)?;
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
            let mut store = Inner::new(file, TEST_SIZE).unwrap();
            let res =
                blocks
                    .into_iter()
                    .map(|block| store.append(block.as_ref())
                        .map(|offset| (offset, block))).collect::<anyhow::Result<Vec<_>>>().unwrap();
            for (offset, block) in res.iter() {
                let actual = store.bytes(*offset).unwrap();
                let expected: &[u8] = block;
                prop_assert_eq!(actual.as_ref(), expected);
            }
            // println!("{:?}", store);
            // for page in store.pages.values() {
            //     println!("{:?}", page);
            // }
            // println!();
        }
    }
}
