//!
//! A radix tree that implements a content-addressed store for ipfs
//!
//!
use fnv::FnvHashSet;
use libipld::{
    cbor::{DagCbor, DagCborCodec},
    codec::Codec,
    multihash::Code,
    raw::RawCodec,
    store::{Store, StoreParams},
    Cid, DefaultParams, Ipld,
};
use log::{info, trace};
use parking_lot::Mutex;
use radixdb::{
    node::{DowncastConverter, IdentityConverter},
    store::{Blob, BlobStore, DynBlobStore, MemStore, NoError, OwnedBlob, PagedFileStore},
    RadixTree, *,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
    sync::Arc,
    time::Instant,
};
use tempfile::tempfile;

#[cfg(test)]
mod tests;

/// alias -> id
const ALIAS: &[u8] = "alias".as_bytes();
/// the actual block data
const BLOCK: &[u8] = "block".as_bytes();

fn format_prefix(prefix: &[u8]) -> anyhow::Result<String> {
    Ok(if prefix.starts_with(ALIAS) {
        format!("\"alias\"   {}", hex::encode(&prefix[BLOCK.len()..]))
    } else if prefix.starts_with(BLOCK) {
        format!("\"block\"   {}", hex::encode(&prefix[BLOCK.len()..]))
    } else {
        hex::encode(prefix)
    })
}

fn format_value(_prefix: &[u8], value: &[u8]) -> anyhow::Result<String> {
    Ok(hex::encode(value))
}

fn alias_key(alias: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(ALIAS.len() + alias.len());
    res.extend_from_slice(ALIAS);
    res.extend_from_slice(alias);
    res
}

fn block_key(id: &Cid) -> Vec<u8> {
    let mut res = Vec::with_capacity(BLOCK.len() + 64);
    res.extend_from_slice(BLOCK);
    id.write_bytes(&mut res).unwrap();
    res
}

#[derive(Debug)]
pub struct Ipfs<S: StoreParams = DefaultParams> {
    root: RadixTree<DynBlobStore>,
    store: DynBlobStore,
    temp_pins: Arc<Mutex<BTreeMap<u64, FnvHashSet<Cid>>>>,
    p: PhantomData<S>,
}

/// a handle that contains a temporary pin
///
/// Dropping this handle enqueues the pin for dropping before the next gc.
// do not implement Clone for this!
pub struct TempPin {
    id: u64,
    temp_pins: Arc<Mutex<BTreeMap<u64, FnvHashSet<Cid>>>>,
}

impl TempPin {
    fn new(id: u64, temp_pins: Arc<Mutex<BTreeMap<u64, FnvHashSet<Cid>>>>) -> Self {
        Self { id, temp_pins }
    }

    fn extend(&mut self, cids: impl IntoIterator<Item = Cid>) {
        let mut temp_pins = self.temp_pins.lock();
        let current = temp_pins.entry(self.id).or_default();
        current.extend(cids);
    }
}

/// dump the temp alias id so you can find it in the database
impl fmt::Debug for TempPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TempAlias");
        if self.id > 0 {
            builder.field("id", &self.id);
        } else {
            builder.field("unused", &true);
        }
        builder.finish()
    }
}

impl Drop for TempPin {
    fn drop(&mut self) {
        if self.id > 0 {
            self.temp_pins.lock().remove(&self.id);
        }
    }
}

trait RadixTreeExt<S: BlobStore> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), S::Error>;

    fn remove(&mut self, key: &[u8]) -> Result<(), S::Error>;
}

impl<S: BlobStore + Clone> RadixTreeExt<S> for RadixTree<S> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), S::Error> {
        self.try_outer_combine_with(&RadixTree::single(key, value), DowncastConverter, |a, b| {
            Ok(a.set(Some(b.downcast())))
        })
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), S::Error> {
        self.try_left_combine_with(&RadixTree::single(key, &[]), DowncastConverter, |a, _| {
            Ok(a.set(None))
        })
    }
}

type Block = libipld::Block<libipld::store::DefaultParams>;

fn get_links<E: Extend<Cid>>(cid: &Cid, data: &[u8], cids: &mut E) -> anyhow::Result<()> {
    let codec = <DefaultParams as StoreParams>::Codecs::try_from(cid.codec())?;
    codec.references::<Ipld, E>(data, cids)?;
    Ok(())
}

impl<S: StoreParams> Ipfs<S> {
    pub fn temp_pin(&mut self) -> TempPin {
        let mut temp_pins = self.temp_pins.lock();
        let id = temp_pins
            .iter()
            .next_back()
            .map(|(id, _)| *id + 1)
            .unwrap_or_default();
        temp_pins.insert(id, Default::default());
        TempPin::new(id, self.temp_pins.clone())
    }

    /// compute the set of all live ids
    pub fn live_set(&self) -> anyhow::Result<FnvHashSet<Cid>> {
        // all ids
        let mut alive = self.gc_roots()?;
        // add temp pins
        for (_, v) in self.temp_pins.lock().iter() {
            alive.extend(v.iter().cloned());
        }
        // new ids (already in all)
        let mut new = alive.clone();
        // a temporary set
        let mut tmp = FnvHashSet::default();
        let mut children = Vec::new();
        while !new.is_empty() {
            std::mem::swap(&mut new, &mut tmp);
            new.clear();
            children.truncate(0);
            for parent in &tmp {
                self.links(parent, &mut children)?;
            }
            for child in &children {
                // add to the new set only if it is a new cid (all.insert returns true)
                if alive.insert(*child) {
                    new.insert(*child);
                }
            }
        }
        Ok(alive)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
    fn get_missing_blocks<C: FromIterator<Cid>>(&self, root: &Cid) -> anyhow::Result<C> {
        let mut curr = FnvHashSet::default();
        curr.insert(*root);
        let mut next = FnvHashSet::default();
        let mut res = Vec::new();
        while !curr.is_empty() {
            for cid in &curr {
                self.links(cid, &mut next)?;
            }
            curr.clear();
            for x in &next {
                if self.has_block(x)? {
                    curr.insert(*x);
                } else {
                    res.push(*x);
                }
            }
        }
        Ok(res.into_iter().collect())
    }

    pub fn gc(&mut self) -> anyhow::Result<()> {
        let t0 = Instant::now();
        info!("figuring out live set");
        let live_set = self.live_set()?;
        let mut dead = self.ids()?;
        let total = dead.len();
        dead.retain(|id| !live_set.contains(id));
        info!(
            "{} total, {} alive, {} s",
            total,
            live_set.len(),
            t0.elapsed().as_secs_f64()
        );
        let t0 = Instant::now();
        info!("taking out the dead");
        for id in dead {
            self.root.remove(&block_key(&id))?;
        }
        info!("{}", t0.elapsed().as_secs_f64());
        Ok(())
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.root.try_reattach()?;
        self.store.sync()
    }

    /// list all aliases
    fn aliases<C: FromIterator<(Vec<u8>, Cid)>>(&self) -> anyhow::Result<C> {
        let res: anyhow::Result<Vec<_>> = self
            .root
            .try_scan_prefix(ALIAS)?
            .map(|r| {
                r.and_then(|(k, v)| {
                    let k = k.to_vec();
                    let v = v
                        .load(&self.store)
                        .and_then(|b| Ok(Cid::read_bytes(b.as_ref())?))?;
                    Ok((k, v))
                })
            })
            .collect();
        Ok(res?.into_iter().collect())
    }

    fn gc_roots(&self) -> anyhow::Result<FnvHashSet<Cid>> {
        self.root
            .try_scan_prefix(ALIAS)?
            .map(|r| {
                r.and_then(|(_, v)| {
                    v.load(&self.store)
                        .and_then(|b| Ok(Cid::read_bytes(b.as_ref())?))
                })
            })
            .collect()
    }

    /// all ids we know of
    fn ids(&self) -> anyhow::Result<Vec<Cid>> {
        self.root
            .try_scan_prefix(BLOCK)?
            .map(|r| r.and_then(|(k, _)| Ok(Cid::read_bytes(&k.as_ref()[5..])?)))
            .collect()
    }

    fn new(store: DynBlobStore) -> anyhow::Result<Self> {
        Ok(Self {
            root: RadixTree::empty(store.clone()),
            store,
            temp_pins: Default::default(),
            p: PhantomData,
        })
    }

    fn get_block(&self, id: &Cid) -> anyhow::Result<Option<OwnedBlob>> {
        let block_key = block_key(id);
        let t = self.root.try_get(&block_key)?;
        let t = t.map(|x| x.load(&self.store)).transpose()?;
        Ok(t)
    }

    pub fn has_block(&self, id: &Cid) -> anyhow::Result<bool> {
        let block_key = block_key(id);
        self.root.try_contains_key(&block_key)
    }

    fn alias(&mut self, name: &[u8], cid: Option<&Cid>) -> anyhow::Result<()> {
        if let Some(cid) = cid {
            self.root.insert(&alias_key(name), &cid.to_bytes())?;
        } else {
            self.root.remove(&alias_key(name))?;
        }
        Ok(())
    }

    fn links<E: Extend<Cid>>(&self, id: &Cid, res: &mut E) -> anyhow::Result<()> {
        let key = block_key(id);
        if let Some(blob) = self.root.try_get(&key)? {
            let blob = blob.load(&self.store)?;
            get_links(id, &blob, res)?;
        };
        Ok(())
    }

    fn dump(&self) -> anyhow::Result<()> {
        todo!()
        // self.root
        //     .dump_tree_custom("", &self.store, format_prefix, format_value, |_| {
        //         Ok("".into())
        //     })
    }

    pub fn put_blocks<'a>(
        &mut self,
        blocks: impl IntoIterator<Item = &'a Block>,
        pin: Option<&mut TempPin>,
    ) -> anyhow::Result<()> {
        let mut cids = Vec::new();
        for block in blocks.into_iter() {
            self.put_block(block, None)?;
            cids.push(*block.cid());
        }
        if let Some(pin) = pin {
            pin.extend(cids);
        }
        Ok(())
    }

    pub fn put_block(&mut self, block: &Block, pin: Option<&mut TempPin>) -> anyhow::Result<()> {
        let block_key = block_key(&block.cid());
        self.root.insert(&block_key, block.data())?;
        if let Some(pin) = pin {
            pin.extend(Some(*block.cid()));
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let file = tempfile()?;
    let store = PagedFileStore::<4194304>::new(file)?;
    let mut ipfs = Ipfs::<DefaultParams>::new(Arc::new(store.clone()))?;
    let mut hashes = Vec::new();
    let blocks = (0..100_000u64)
        .map(|i| {
            let mut data = vec![0u8; 10000];
            data[0..8].copy_from_slice(&i.to_be_bytes());
            Block::encode(DagCborCodec, Code::Sha2_256, &Ipld::Bytes(data)).unwrap()
        })
        .collect::<Vec<_>>();
    let t0 = Instant::now();
    for (i, block) in blocks.into_iter().enumerate() {
        println!("putting block {}", i);
        ipfs.put_block(&block, None)?;
        hashes.push(*block.cid());
    }
    println!("finished {}", t0.elapsed().as_secs_f64());
    let t0 = Instant::now();
    println!("committing {:?}", store);
    ipfs.commit()?;
    println!("done {:?} {}", store, t0.elapsed().as_secs_f64());
    // ipfs.dump()?;
    // let data = ipfs.get(b"abcd")?;
    // ipfs.alias(b"root1", Some(b"abcd"))?;
    // ipfs.dump()?;
    println!("{:?}", ipfs.live_set()?);
    ipfs.alias(b"root1", None)?;
    println!("traversing all (in order)");
    let t0 = Instant::now();
    let mut res = 0u64;
    for hash in &hashes {
        res += ipfs.get_block(hash)?.unwrap().len() as u64;
    }
    println!("done {} {}", res, t0.elapsed().as_secs_f64());
    println!("traversing all (in order, hot)");
    let t0 = Instant::now();
    let mut res = 0u64;
    for hash in &hashes {
        res += ipfs.get_block(hash)?.unwrap().len() as u64;
    }
    println!("done {} {}", res, t0.elapsed().as_secs_f64());
    hashes.sort();
    for _ in 0..10 {
        println!("traversing all (random)");
        let t0 = Instant::now();
        let mut res = 0u64;
        for hash in &hashes {
            res += ipfs.get_block(hash)?.unwrap().len() as u64;
        }
        println!("done {} {}", res, t0.elapsed().as_secs_f64());
    }
    // ipfs.dump()?;
    println!("performing gc");
    ipfs.gc()?;
    println!("committing {:?}", store);
    ipfs.commit()?;
    println!("done {:?}", store);
    // ipfs.dump()?;
    Ok(())
}
