use anyhow::Context;
use futures::{channel::mpsc, FutureExt, StreamExt};
use js_sys::{Array, ArrayBuffer, Uint8Array};
use log::{info, trace};
use num_traits::Num;
use radixdb::{Blob, BlobOwner};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ops::Range,
    str::FromStr,
    sync::Arc,
};
use url::Url;
use wasm_bindgen::JsValue;
use wasm_futures_executor::ThreadPool;
use web_sys::{Cache, CacheQueryOptions, Request, Response};

use crate::{
    js_async, overlap, ranges, sync_fs::SyncFs, worker_scope, Command, JsError, SharedStr,
};

/// A virtual file system that uses the browser cache as storage
pub struct WebCacheFs {
    dirs: BTreeMap<SharedStr, (WebCacheDir, BTreeMap<SharedStr, WebCacheFile>)>,
    queue: mpsc::UnboundedReceiver<Command>,
}

impl WebCacheFs {
    /// create a new WebCacheFs on a separate thread, and wrap it in a SyncFs
    pub fn new(pool: ThreadPool) -> SyncFs {
        let (tx, rx) = mpsc::unbounded();
        pool.spawn_lazy(|| {
            Self {
                queue: rx,
                dirs: Default::default(),
            }
            .run()
            .boxed_local()
        });
        SyncFs::new(tx)
    }

    async fn open_dir(&mut self, dir_name: impl Into<SharedStr>) -> anyhow::Result<()> {
        let dir_name = dir_name.into();
        if !self.dirs.contains_key(&dir_name) {
            let dir = WebCacheDir::new(&dir_name).await?;
            self.dirs.insert(dir_name, (dir, Default::default()));
        }
        Ok(())
    }

    async fn open_file(&mut self, dir_name: SharedStr, file_name: SharedStr) -> anyhow::Result<()> {
        let (dir, files) = self.dirs.get_mut(&dir_name).context("dir not open")?;
        if !files.contains_key(&file_name) {
            let wcf = dir.open_file(file_name.clone()).await?;
            files.insert(file_name, wcf);
        }
        Ok(())
    }

    async fn append_file_length_prefixed(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.append_length_prefixed(data).await
    }

    async fn flush_file(&mut self, dir: SharedStr, file_name: SharedStr) -> anyhow::Result<()> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.flush().await
    }

    async fn read_file_length_prefixed(
        &mut self,
        dir: SharedStr,
        file_name: SharedStr,
        offset: u64,
    ) -> anyhow::Result<Blob<u8>> {
        let (_, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let file = files.get_mut(&file_name).context("file not open")?;
        file.load_length_prefixed(offset).await
    }

    async fn delete_file(&mut self, dir: SharedStr, file_name: SharedStr) -> anyhow::Result<bool> {
        let (dir, files) = self.dirs.get_mut(&dir).context("dir not open")?;
        let res = dir.delete(&file_name).await?;
        files.remove(&file_name);
        Ok(res)
    }

    async fn delete_dir(&mut self, dir_name: SharedStr) -> anyhow::Result<bool> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let res: JsValue = js_async(caches.delete(&dir_name)).await?;
        Ok(res.as_bool().context("no bool")?)
    }

    async fn run(mut self) {
        while let Some(cmd) = self.queue.next().await {
            match cmd {
                Command::OpenDir { dir_name, cb } => {
                    let _ = cb.send(self.open_dir(dir_name).await);
                }
                Command::DeleteDir { dir_name, cb } => {
                    let _ = cb.send(self.delete_dir(dir_name).await);
                }
                Command::ReadFileLengthPrefixed {
                    dir_name,
                    file_name,
                    offset,
                    cb,
                } => {
                    let _ = cb.send(
                        self.read_file_length_prefixed(dir_name, file_name, offset)
                            .await,
                    );
                }
                Command::AppendFileLengthPrefixed {
                    dir_name,
                    file_name,
                    data,
                    cb,
                } => {
                    let _ = cb.send(
                        self.append_file_length_prefixed(dir_name, file_name, data)
                            .await,
                    );
                }
                Command::FlushFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.flush_file(dir_name, file_name).await);
                }
                Command::OpenFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.open_file(dir_name, file_name).await);
                }
                Command::DeleteFile {
                    dir_name,
                    file_name,
                    cb,
                } => {
                    let _ = cb.send(self.delete_file(dir_name, file_name).await);
                }
                Command::Shutdown => {
                    // shut down the loop,
                    // which will shut down the thread,
                    // which will drop the mpsc receiver,
                    // which will cause all file ops to fail.
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebCacheDir {
    /// the cache which we abuse as a directory
    cache: web_sys::Cache,
}

impl WebCacheDir {
    pub(crate) async fn new(dir_name: &str) -> anyhow::Result<Self> {
        let caches = worker_scope().caches().map_err(JsError::from)?;
        let cache = js_async(caches.open(dir_name)).await?;
        Ok(Self { cache })
    }

    pub(crate) async fn open_file(&self, file_name: SharedStr) -> anyhow::Result<WebCacheFile> {
        WebCacheFile::new(self.cache.clone(), file_name).await
    }

    pub(crate) async fn delete(&self, file_name: &str) -> anyhow::Result<bool> {
        let res: JsValue = js_async(
            self.cache.delete_with_str_and_options(
                &format!("/{}", file_name),
                CacheQueryOptions::new()
                    .ignore_method(true)
                    .ignore_search(true),
            ),
        )
        .await?;
        Ok(res.as_bool().context("not a bool")?)
    }
}

#[derive(Debug)]
struct Page(Arc<[u8]>);

impl BlobOwner for Page {
    fn get_slice(&self, offset: usize) -> &[u8] {
        let data = &self.0;
        let base = offset + 4;
        let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        &data[base..base + length]
    }

    fn is_valid(&self, offset: usize) -> bool {
        let data = &self.0;
        if offset + 4 <= data.len() {
            let length = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset + 4 + length <= data.len()
        } else {
            false
        }
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

#[derive(Debug)]
pub struct WebCacheFile {
    /// the cache (basically the directory where the file lives in)
    cache: web_sys::Cache,
    /// the file name
    file_name: SharedStr,
    /// immutable full pages
    full_pages: BTreeMap<u64, Arc<dyn BlobOwner>>,
    /// appendable last page
    last_page: Arc<Vec<u8>>,
    /// the current last page, as a dyn
    last_page_dyn: Option<Arc<dyn BlobOwner>>,
    /// all the chunks that we have, sorted
    chunks: Vec<ChunkMeta>,
    /// page size
    page_size: u64,
    /// current length of the file
    length: u64,
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

impl WebCacheFile {
    pub(crate) async fn new(cache: web_sys::Cache, file_name: SharedStr) -> anyhow::Result<Self> {
        let page_size = 1024 * 1024;
        let mut chunks = chunks(&cache, &file_name).await?;
        let to_delete = retain_relevant(&mut chunks);
        delete(&cache, &file_name, &to_delete).await?;
        // length is simply the highest offset of a still valid chunk
        let length = chunks
            .iter()
            .map(|x| x.range().end)
            .max()
            .unwrap_or_default();
        // load the last page
        let last_page_range = page_range(length, page_size);
        let last_page: Arc<Vec<u8>> =
            load_chunks(&cache, &file_name, last_page_range, chunks.iter())
                .await?
                .into();
        Ok(Self {
            cache,
            file_name,
            chunks,
            full_pages: Default::default(),
            last_page,
            last_page_dyn: Default::default(),
            page_size,
            length,
        })
    }

    fn prev(&self) -> ChunkMeta {
        self.chunks.last().cloned().unwrap_or_default()
    }

    // close the current page, increasing offset and adding it to the set of full pages
    //
    // this will also write the current page
    async fn close_page(&mut self) -> anyhow::Result<()> {
        let page0 = page_num(self.length, self.page_size);
        let page_range = page_range(self.length, self.page_size);
        info!("closing page {} {:?}", page0, page_range);
        let chunk = self.mk_chunk(page_range.clone());
        let vec = Arc::make_mut(&mut self.last_page);
        write_chunk(&self.cache, &self.file_name, &chunk, vec).await?;
        self.chunks.push(chunk);
        let to_delete = retain_relevant(&mut self.chunks);
        delete(&self.cache, &self.file_name, &to_delete).await?;
        let page = Page(vec.as_slice().into());
        let page: Arc<dyn BlobOwner> = Arc::new(page);
        self.full_pages.insert(page0, page);
        self.length = page_range.end;
        // clear the page
        vec.fill(0u8);
        // clear the last page dyn, since it is not valid anymore
        self.last_page_dyn = None;
        let page1 = page_num(self.length, self.page_size);
        assert!(page0 + 1 == page1);
        Ok(())
    }

    fn mk_chunk(&self, range: Range<u64>) -> ChunkMeta {
        let overwrite = intersecting_chunks(&self.chunks, range.clone())
            .next()
            .is_some();
        let level = if overwrite {
            self.prev().level + 1
        } else {
            self.prev().level
        };
        ChunkMeta {
            level,
            offset: range.start,
            len: range.end.saturating_sub(range.start),
        }
    }

    pub(crate) async fn flush(&mut self) -> anyhow::Result<()> {
        let page_num = page_num(self.length, self.page_size);
        let page_start = page_range(self.length, self.page_size).start;
        let page_end = self.length;
        let page_range = page_start..page_end;
        let offset = offset_within_page(self.length, self.page_size);
        info!("flush page {} {:?}", page_num, page_range);
        let chunk = self.mk_chunk(page_range);
        let last_page = Arc::make_mut(&mut self.last_page);
        write_chunk(
            &self.cache,
            &self.file_name,
            &chunk,
            &mut last_page[0..offset],
        )
        .await?;
        self.chunks.push(chunk);
        let to_delete = retain_relevant(&mut self.chunks);
        delete(&self.cache, &self.file_name, &to_delete).await?;
        Ok(())
    }

    fn remaining(&self) -> u64 {
        page_range(self.length, self.page_size).end - self.length
    }

    async fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        let len = u64::try_from(data.len())?;
        anyhow::ensure!(len <= self.page_size, "item too large");
        if len + 8 > self.remaining() {
            self.close_page().await?;
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

    pub(crate) async fn append_length_prefixed(
        &mut self,
        mut data: Vec<u8>,
    ) -> anyhow::Result<u64> {
        // splice in the length
        data.splice(0..0, u32::try_from(data.len()).unwrap().to_be_bytes());
        // append the data.
        self.append(&mut data).await
    }

    pub(crate) async fn load_length_prefixed(&mut self, start: u64) -> anyhow::Result<Blob<u8>> {
        let last_page = page_num(self.length, self.page_size);
        let page_num = page_num(start, self.page_size);
        // offset of start within its page
        let offset = offset_within_page(start, self.page_size);
        let page: &Arc<dyn BlobOwner> = if page_num == last_page {
            trace!("hit to last page {}", page_num);
            // end of the validity of the current page
            let end = offset_within_page(self.length, self.page_size);
            let last_page = LastPage(self.last_page.clone(), end);
            self.last_page_dyn
                .get_or_insert_with(|| Arc::new(last_page))
        } else if let Some(page) = self.full_pages.get(&page_num) {
            trace!("hit to cached page {}", page_num);
            page
        } else {
            anyhow::ensure!(page_num < last_page);
            trace!("cache miss {}", page_num);
            let page_range = page_range(start, self.page_size);
            let chunks = intersecting_chunks(&self.chunks, page_range.clone());
            let page: Arc<[u8]> = load_chunks(&self.cache, &self.file_name, page_range, chunks)
                .await?
                .into();
            let page = Page(page);
            let page: Arc<dyn BlobOwner> = Arc::new(page);
            self.full_pages.insert(page_num, page);
            self.full_pages.get(&page_num).unwrap()
        };
        // Ok(Blob::arc_from_byte_slice(page.get_slice(offset)))
        Blob::custom(page.clone(), offset)
    }

    async fn load(&self, chunk: &ChunkMeta) -> anyhow::Result<Vec<u8>> {
        let request: String = chunk.to_request_str(&self.file_name);
        let response: Response = js_async(self.cache.match_with_str(&request)).await?;
        let ab: ArrayBuffer = js_async(response.array_buffer().map_err(JsError::from)?).await?;
        Ok(Uint8Array::new(&ab).to_vec())
    }
}

async fn write_chunk(
    cache: &Cache,
    file_name: &str,
    chunk: &ChunkMeta,
    data: &mut [u8],
) -> anyhow::Result<()> {
    let request = chunk.to_request_str(file_name);
    let response = Response::new_with_opt_u8_array(Some(data)).map_err(JsError::from)?;
    let _: JsValue = js_async(cache.put_with_str(&request, &response)).await?;
    Ok(())
}

fn intersecting_chunks<'a>(
    chunks: impl IntoIterator<Item = &'a ChunkMeta>,
    range: Range<u64>,
) -> impl Iterator<Item = &'a ChunkMeta> {
    chunks
        .into_iter()
        .filter(move |chunk| overlap(chunk.range(), range.clone()))
}

/// sort chunks, retain relevant, and return the ones to be deletes as a vec
fn retain_relevant(chunks: &mut Vec<ChunkMeta>) -> Vec<ChunkMeta> {
    chunks.sort();
    let mut prev = ChunkMeta::default();
    let mut to_delete = Vec::new();
    chunks.retain(|chunk| {
        if prev.level == chunk.level {
            // same level as previous
            if prev.range().end == chunk.range().start {
                // consecutive, move ahead
                prev = *chunk;
                true
            } else {
                // non-consecutive, discard
                to_delete.push(*chunk);
                false
            }
        } else if prev.level + 1 == chunk.level {
            if chunk.range().start <= prev.range().end {
                // consecutive with previous level, take
                prev = *chunk;
                true
            } else {
                // chunk is not consecutive, discard
                to_delete.push(*chunk);
                false
            }
        } else {
            to_delete.push(*chunk);
            false
        }
    });
    to_delete
}

async fn delete(
    cache: &web_sys::Cache,
    file_name: &str,
    to_delete: &[ChunkMeta],
) -> anyhow::Result<()> {
    for chunk in to_delete {
        let request = chunk.to_request_str(file_name);
        let _: JsValue = js_async(cache.delete_with_str(&request)).await?;
    }
    Ok(())
}

async fn load_chunks(
    cache: &Cache,
    file_name: &str,
    range: Range<u64>,
    chunks: impl IntoIterator<Item = &ChunkMeta>,
) -> anyhow::Result<Vec<u8>> {
    let chunks = chunks
        .into_iter()
        .filter(|chunk| overlap(chunk.range(), range.clone()));
    // let t0 = js_sys::Date::now();
    let len = range.end.saturating_sub(range.start).try_into()?;
    let mut res = vec![0u8; len];
    for chunk in chunks {
        let (sr, tr) = ranges(chunk.range(), range.clone())?;
        // let t0 = js_sys::Date::now();
        let data = load_range(cache, file_name, chunk, sr).await?;
        // info!("load_range {}", js_sys::Date::now() - t0);
        data.copy_to(&mut res[tr]);
    }
    // info!("load_chunks {}", js_sys::Date::now() - t0);
    Ok(res)
}

async fn load_range(
    cache: &Cache,
    file_name: &str,
    chunk: &ChunkMeta,
    range: Range<usize>,
) -> anyhow::Result<Uint8Array> {
    let request: String = chunk.to_request_str(file_name);
    // let t0 = js_sys::Date::now();
    let response: Response = js_async(cache.match_with_str(&request)).await?;
    // info!("get response {}", js_sys::Date::now() - t0);
    const USE_BLOB: bool = true;
    let ab = if USE_BLOB {
        // let t0 = js_sys::Date::now();
        let blob: web_sys::Blob = js_async(response.blob().map_err(JsError::from)?).await?;
        // info!("load blob {}", js_sys::Date::now() - t0);
        // let t0 = js_sys::Date::now();
        let slice = blob
            .slice_with_i32_and_i32(range.start.try_into()?, range.end.try_into()?)
            .map_err(JsError::from)?;
        // info!("slice blob {}", js_sys::Date::now() - t0);
        // let t0 = js_sys::Date::now();
        let res: ArrayBuffer = js_async(slice.array_buffer()).await?;
        // info!("blob -> arraybuffer {}", js_sys::Date::now() - t0);
        res
    } else {
        let ab: ArrayBuffer = js_async(response.array_buffer().map_err(JsError::from)?).await?;
        ab.slice_with_end(range.start.try_into()?, range.end.try_into()?)
    };
    // let t0 = js_sys::Date::now();
    let ua = Uint8Array::new(&ab);
    // info!("convert to uint8array {}", js_sys::Date::now() - t0);
    Ok(ua)
}

async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<Vec<ChunkMeta>> {
    let keys: JsValue = js_async(cache.keys()).await?;
    let chunks = Array::from(&keys)
        .iter()
        .filter_map(|req| ChunkMeta::parse_and_filter(&Request::from(req).url(), name).ok())
        .filter_map(|x| x)
        .collect::<Vec<_>>();
    Ok(chunks)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
/// metadata for a chunk
struct ChunkMeta {
    /// level - gets increased on truncate
    ///
    /// this is the only way to do atomic truncate with a non atomic blob store like the web-sys cache
    level: u64,
    /// offset
    offset: u64,
    /// len
    len: u64,
}

impl ChunkMeta {
    fn range(&self) -> Range<u64> {
        self.offset..(self.offset + self.len)
    }

    fn parse_and_filter(request: &str, file_name: &str) -> anyhow::Result<Option<ChunkMeta>> {
        // info!("parse_and_filter {}", request);
        let url = Url::parse(request)?;
        if let Some(query) = url.query() {
            let name = url.path().strip_prefix('/').unwrap();
            // info!("parse_and_filter name {} file_name {}  query {}", name, file_name, query);
            if name != file_name {
                // not ours
                Ok(None)
            } else {
                let meta = ChunkMeta::from_str(query)?;
                // info!("chunk {:?}", meta);
                Ok(Some(meta))
            }
        } else {
            Ok(None)
        }
    }

    fn to_request_str(&self, file_name: &str) -> String {
        format!(
            "/{}?{:016x}-{:016x}-{:016x}",
            file_name, self.level, self.offset, self.len
        )
    }
}

impl ToString for ChunkMeta {
    fn to_string(&self) -> String {
        format!("{:016x}-{:016x}-{:016x}", self.level, self.offset, self.len)
    }
}

impl FromStr for ChunkMeta {
    type Err = anyhow::Error;

    fn from_str(args: &str) -> Result<Self, Self::Err> {
        let parts = args.split('-').collect::<Vec<&str>>();
        anyhow::ensure!(parts.len() == 3);
        let level = u64::from_str_radix(parts[0], 16)?;
        let offset = u64::from_str_radix(parts[1], 16)?;
        let len = u64::from_str_radix(parts[2], 16)?;
        Ok(Self { level, offset, len })
    }
}
