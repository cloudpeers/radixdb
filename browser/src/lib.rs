use std::convert::TryInto;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{channel::mpsc, executor::block_on};
use futures::{Future, FutureExt, StreamExt};
use js_sys::Array;
use log::*;
use radixdb::{Blob, BlobStore};
use url::Url;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::{Cache, DedicatedWorkerGlobalScope};
use web_sys::{CacheQueryOptions, Request, Response};

#[derive(Debug)]
struct WebCacheFile {
    /// the cache (basically the directory where the file lives in)
    cache: web_sys::Cache,
    /// the file name
    file_name: String,
    /// all the chunks that we have, sorted
    chunks: Vec<ChunkMeta>,
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
        self.offset..self.len
    }

    fn parse_and_filter(request: &Request, file_name: &str) -> anyhow::Result<Option<ChunkMeta>> {
        let url = request.url();
        let parts = url.split('?').take(2).collect::<Vec<_>>();
        if let Some(query) = parts.get(1) {
            let name = parts[0].strip_prefix("/").unwrap();
            anyhow::ensure!(name == file_name);
            let meta = ChunkMeta::from_str(query)?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn to_request_str(&self, file_name: &str) -> String {
        format!(
            "{}/{:016x}-{:016x}-{:016x}",
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
        let parts = args.split('_').collect::<Vec<&str>>();
        anyhow::ensure!(parts.len() == 3);
        let lvl = u64::from_str_radix(parts[0], 16)?;
        let ofs = u64::from_str_radix(parts[1], 16)?;
        let len = u64::from_str_radix(parts[2], 16)?;
        Ok(Self {
            offset: ofs,
            len,
            level: lvl,
        })
    }
}

impl WebCacheFile {
    async fn new(cache: web_sys::Cache, file_name: String) -> anyhow::Result<Self> {
        let mut chunks = Self::chunks(&cache, &file_name).await?;
        let to_delete = Self::retain_relevant(&mut chunks);
        Self::delete(&cache, &file_name, &to_delete).await?;
        Ok(Self {
            cache,
            file_name,
            chunks,
        })
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
                if chunk.offset == 0 {
                    // new level starts at offset 0, take
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
            JsFuture::from(cache.delete_with_str(&request))
                .await
                .map_err(js_to_anyhow)?;
        }
        Ok(())
    }

    async fn chunks(cache: &web_sys::Cache, name: &str) -> anyhow::Result<Vec<ChunkMeta>> {
        let keys = JsFuture::from(cache.keys()).await.map_err(js_to_anyhow)?;
        let chunks = Array::from(&keys)
            .iter()
            .filter_map(|req| ChunkMeta::parse_and_filter(&Request::from(req), name).ok())
            .filter_map(|x| x)
            .collect::<Vec<_>>();
        Ok(chunks)
    }

    fn prev(&self) -> ChunkMeta {
        self.chunks.last().cloned().unwrap_or_default()
    }

    async fn append(&mut self, data: &mut Vec<u8>) -> anyhow::Result<()> {
        let prev = self.prev();
        let chunk = ChunkMeta {
            level: prev.level,
            offset: prev.range().end,
            len: data.len().try_into()?,
        };
        let request = chunk.to_request_str(&self.file_name);
        let response = Response::new_with_opt_u8_array(Some(data)).map_err(js_to_anyhow)?;
        JsFuture::from(self.cache.put_with_str(&request, &response))
            .await
            .expect("insertion not possible");
        self.chunks.push(chunk);
        Ok(())
    }

    async fn read(offset: u64, length: u64) -> anyhow::Result<Vec<u8>> {
        todo!()
    }
}

async fn keys(cache: &web_sys::Cache) -> std::result::Result<Vec<String>, JsValue> {
    let keys = JsFuture::from(cache.keys()).await?;
    let names = Array::from(&keys)
        .iter()
        .map(|req| Request::from(req).url())
        .filter_map(|url| {
            Url::parse(&url)
                .ok()
                .and_then(|x| x.path().strip_prefix("/").map(|x| x.to_owned()))
        })
        .collect::<Vec<_>>();
    Ok(names)
}

// #[wasm_bindgen(start)]
// pub fn main() {
//     let _ = console_log::init_with_level(log::Level::Debug);
//     ::console_error_panic_hook::set_once();
// }

fn worker_scope() -> DedicatedWorkerGlobalScope {
    js_sys::global().unchecked_into::<DedicatedWorkerGlobalScope>()
}

fn worker_name() -> String {
    worker_scope().name().as_str().to_string()
}

fn js_to_anyhow(js: JsValue) -> anyhow::Error {
    anyhow::anyhow!("Damn {:?}", js)
}

fn err_to_jsvalue(value: impl std::error::Error) -> JsValue {
    value.to_string().into()
}

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let pool = ThreadPool::new(2).await?;
    let pool2 = pool.clone();
    pool.spawn_lazy(|| {
        async {
            let cache: web_sys::Cache =
                JsFuture::from(worker_scope().caches().unwrap().open("cache"))
                    .await
                    .unwrap()
                    .into();
        }
        .boxed_local()
    });
    Ok(0.into())
}
