use super::{blob_store::OwnedBlob, Blob, BlobStore};
use anyhow::Context;
use parking_lot::Mutex;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

/// A simple in memory store
#[derive(Default, Clone)]
pub struct MemStore {
    data: Arc<Mutex<BTreeMap<u64, Arc<Vec<u8>>>>>,
}
impl MemStore {
    pub fn count(&self) -> usize {
        self.data.lock().len()
    }
}

impl Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.lock();
        f.debug_struct("MemStore")
            .field("count", &data.len())
            .finish()
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
