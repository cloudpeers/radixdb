use super::Hex;
use anyhow::Context;
use lazy_static::lazy_static;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

pub trait BlobStore: Debug + Send + Sync {
    fn bytes(&self, id: u64) -> anyhow::Result<&[u8]>;

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64>;
}

pub type DynBlobStore = Box<dyn BlobStore>;

#[derive(Default, Debug, Clone)]
pub struct NoStore;

impl NoStore {
    pub fn new() -> DynBlobStore {
        Box::new(Self)
    }
}

impl BlobStore for NoStore {
    fn bytes(&self, _: u64) -> anyhow::Result<&[u8]> {
        anyhow::bail!("no store");
    }

    fn append(&mut self, _: &[u8]) -> anyhow::Result<u64> {
        anyhow::bail!("no store");
    }
}

lazy_static! {
    /// A noop store, for when we know that a tree is not attached
    pub static ref NO_STORE: DynBlobStore = NoStore::new();
}

#[derive(Default, Clone)]
pub struct MemStore {
    data: BTreeMap<u64, Arc<Vec<u8>>>,
}

impl Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_map();
        for (id, v) in &self.data {
            builder.entry(&id, &Hex(v.as_ref()));
        }
        builder.finish()
    }
}
impl BlobStore for MemStore {
    fn bytes(&self, id: u64) -> anyhow::Result<&[u8]> {
        self.data
            .get(&id)
            .map(|x| x.as_ref().as_ref())
            .context("value not found")
    }

    fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        let max = self.data.keys().next_back().cloned().unwrap_or(0);
        let id = max + 1;
        let data = Arc::new(data.to_vec());
        self.data.insert(id, data);
        Ok(id)
    }
}
