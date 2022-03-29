use log::{debug, info, trace};
use num_traits::Num;
use parking_lot::Mutex;
use radixdb::{Blob, BlobOwner, BlobStore};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ops::Range,
    sync::Arc,
};

use crate::SyncFile;

#[derive(Debug)]
struct PagingFileInner {
    /// the syncfile we are wrapping
    inner: SyncFile,
    /// immutable full pages
    full_pages: BTreeMap<u64, Arc<dyn BlobOwner>>,
    /// appendable last page
    last_page: Arc<Vec<u8>>,
    /// the current last page, as a dyn
    last_page_dyn: Option<Arc<dyn BlobOwner>>,
    /// page size
    page_size: u64,
    /// current length of the file
    length: u64,
}

impl PagingFileInner {
    pub fn new(inner: SyncFile, page_size: u64) -> anyhow::Result<Self> {
        let length = inner.length()?;
        let last_page_range = page_range(length, page_size);
        let mut last_page = inner.read(last_page_range.start..length)?;
        let remaining = last_page_range.end - length;
        last_page.extend((0..remaining).map(|_| 0u8));
        let last_page = Arc::new(last_page);
        Ok(Self {
            inner,
            page_size,
            last_page,
            length,
            full_pages: BTreeMap::new(),
            last_page_dyn: None,
        })
    }

    // close the current page, increasing offset and adding it to the set of full pages
    //
    // this will also write the current page
    fn close_page(&mut self) -> anyhow::Result<()> {
        let page0 = page_num(self.length, self.page_size);
        let page_range = page_range(self.length, self.page_size);
        debug!("closing page {} {:?}", page0, page_range);
        let last_page = Arc::make_mut(&mut self.last_page);
        self.inner.write(page_range.start, last_page.clone())?;
        let page = Page(last_page.as_slice().into());
        let page: Arc<dyn BlobOwner> = Arc::new(page);
        self.full_pages.insert(page0, page);
        self.length = page_range.end;
        // clear the page
        last_page.fill(0u8);
        // clear the last page dyn, since it is not valid anymore
        self.last_page_dyn = None;
        let page1 = page_num(self.length, self.page_size);
        assert!(page0 + 1 == page1);
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> anyhow::Result<()> {
        let page_num = page_num(self.length, self.page_size);
        let page_start = page_range(self.length, self.page_size).start;
        let page_end = self.length;
        let page_range = page_start..page_end;
        let offset = offset_within_page(self.length, self.page_size);
        debug!("flushing page {} {:?}", page_num, page_range);
        self.inner
            .write(page_start, self.last_page[..offset].to_vec())?;
        Ok(())
    }

    fn remaining(&self) -> u64 {
        page_range(self.length, self.page_size).end - self.length
    }

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        let len = u64::try_from(data.len())?;
        anyhow::ensure!(len <= self.page_size, "item too large");
        if len + 8 > self.remaining() {
            self.close_page()?;
        }
        // make sure base is aligned
        if !is_aligned(self.length + 4, ALIGNMENT) {
            let missing = align(self.length + 4, ALIGNMENT).end - self.length - 4;
            self.length += missing;
        }
        let last_page = Arc::make_mut(&mut self.last_page);
        // offset within the page
        let offset = offset_within_page(self.length, self.page_size);
        // copy into the page
        last_page[offset..offset + data.len()].copy_from_slice(data);
        // clear the last page dyn, since it is not valid anymore
        self.last_page_dyn = None;
        let res = self.length;
        self.length += len;
        Ok(res)
    }

    pub(crate) fn append_length_prefixed(&mut self, mut data: Vec<u8>) -> anyhow::Result<u64> {
        // splice in the length
        data.splice(0..0, u32::try_from(data.len()).unwrap().to_be_bytes());
        // append the data.
        self.append(&mut data)
    }

    pub(crate) fn load_length_prefixed(&mut self, start: u64) -> anyhow::Result<Blob<u8>> {
        let last_page = page_num(self.length, self.page_size);
        let page_num = page_num(start, self.page_size);
        // offset of start within its page
        let offset = offset_within_page(start, self.page_size);
        let page: &Arc<dyn BlobOwner> = if page_num == last_page {
            // trace!("hit to last page {}", page_num);
            // end of the validity of the current page
            let end = offset_within_page(self.length, self.page_size);
            let last_page = LastPage(self.last_page.clone(), end);
            self.last_page_dyn
                .get_or_insert_with(|| Arc::new(last_page))
        } else if let Some(page) = self.full_pages.get(&page_num) {
            // trace!("hit to cached page {}", page_num);
            page
        } else {
            anyhow::ensure!(page_num < last_page);
            // trace!("cache miss {}", page_num);
            let page_range = page_range(start, self.page_size);
            let page: Arc<[u8]> = self.inner.read(page_range)?.into();
            let page = Page(page);
            let page: Arc<dyn BlobOwner> = Arc::new(page);
            self.full_pages.insert(page_num, page);
            self.full_pages.get(&page_num).unwrap()
        };
        Blob::custom(page.clone(), offset)
    }
}

#[derive(Debug)]
pub struct PagingFile(Arc<parking_lot::Mutex<PagingFileInner>>);

impl PagingFile {
    pub fn new(inner: SyncFile, page_size: u64) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(PagingFileInner::new(
            inner, page_size,
        )?))))
    }
}

impl BlobStore for PagingFile {
    fn read(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        self.0.lock().load_length_prefixed(id)
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<u64> {
        self.0.lock().append_length_prefixed(data.to_vec())
    }

    fn sync(&self) -> anyhow::Result<()> {
        self.0.lock().sync()
    }
}

/// given an offset and a page size, return the page number
fn page_num<T: Num + Copy>(offset: T, page_size: T) -> T {
    offset / page_size
}

/// given an offset and a page size, give the offset range of a page
fn page_range<T: Num + Copy>(offset: T, page_size: T) -> Range<T> {
    let page = page_num(offset, page_size);
    let start = page * page_size;
    let end = start + page_size;
    start..end
}

fn offset_within_page(offset: u64, page_size: u64) -> usize {
    let page_range = page_range(offset, page_size);
    usize::try_from(offset.saturating_sub(page_range.start)).unwrap()
}

/// true if the offset is aligned to the alignment
fn is_aligned<T: Num + Copy>(offset: T, alignment: T) -> bool {
    offset % alignment == T::zero()
}

/// given an offset, calculate the aligned offset (start and end)
fn align<T: Num + Copy>(offset: T, alignment: T) -> Range<T> {
    if is_aligned(offset, alignment) {
        offset..offset
    } else {
        let start = (offset / alignment) * alignment;
        let end = start + alignment;
        start..end
    }
}

const ALIGNMENT: u64 = 8;

#[derive(Debug)]
struct Page(Arc<[u8]>);

impl BlobOwner for Page {
    fn is_valid(&self, offset: usize) -> bool {
        let data = &self.0;
        if offset + 4 <= data.len() {
            let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset + 4 + length <= data.len()
        } else {
            false
        }
    }

    fn get_slice(&self, offset: usize) -> &[u8] {
        let data = &self.0;
        let base = offset + 4;
        let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        &data[base..base + length]
    }
}

#[derive(Debug)]
struct LastPage(Arc<Vec<u8>>, usize);

impl BlobOwner for LastPage {
    fn get_slice(&self, offset: usize) -> &[u8] {
        let data = &self.0;
        let base = offset + 4;
        let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        &data[base..base + length]
    }

    fn is_valid(&self, offset: usize) -> bool {
        let data = &self.0;
        let end = self.1;
        if offset + 4 <= end {
            let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset + 4 + length <= data.len()
        } else {
            false
        }
    }
}
