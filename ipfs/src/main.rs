//!
//! A radix tree that implements a content-addressed store for ipfs
//!
//!
use libipld::{
    cbor::{DagCbor, DagCborCodec},
    codec::Codec,
    multihash::Code,
    raw::RawCodec,
    store::StoreParams,
    Cid, DefaultParams, Ipld,
};
use radixdb::{
    node::{DowncastConverter, IdentityConverter},
    store::{Blob, BlobStore, DynBlobStore, MemStore, NoError, OwnedBlob, PagedFileStore},
    RadixTree, *,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use tempfile::tempfile;

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
    let mut res = Vec::with_capacity(BLOCK.len());
    res.extend_from_slice(BLOCK);
    id.write_bytes(&mut res).unwrap();
    res
}

struct Ipfs {
    root: RadixTree<DynBlobStore>,
    store: DynBlobStore,
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

fn get_links(cid: &Cid, data: &[u8], cids: &mut Vec<Cid>) -> anyhow::Result<()> {
    let codec = <DefaultParams as StoreParams>::Codecs::try_from(cid.codec())?;
    codec.references::<Ipld, Vec<Cid>>(data, cids)?;
    Ok(())
}

impl Ipfs {
    /// compute the set of all live ids
    fn live_set(&self) -> anyhow::Result<BTreeSet<Cid>> {
        // all ids
        let mut all = self.aliases()?;
        // todo: add temp pins!
        // new ids (already in all)
        let mut new = all.clone();
        // a temporary set
        let mut tmp = BTreeSet::default();
        while !new.is_empty() {
            std::mem::swap(&mut new, &mut tmp);
            new.clear();
            for parent in &tmp {
                if let Some(children) = self.links(parent)? {
                    for child in children {
                        if !all.insert(child) {
                            new.insert(child);
                        }
                    }
                }
            }
        }
        Ok(all)
    }

    fn gc(&mut self) -> anyhow::Result<()> {
        let t0 = Instant::now();
        println!("figuring out live set");
        let live_set = self.live_set()?;
        let mut dead = self.ids()?;
        dead.retain(|id| !live_set.contains(id));
        println!("{}", t0.elapsed().as_secs_f64());
        let t0 = Instant::now();
        println!("taking out the dead");
        for id in dead {
            self.root.remove(&block_key(&id))?;
        }
        println!("{}", t0.elapsed().as_secs_f64());
        Ok(())
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        self.root.try_reattach()?;
        self.store.sync()
    }

    fn aliases(&self) -> anyhow::Result<BTreeSet<Cid>> {
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
        })
    }

    fn get_block(&self, id: &Cid) -> anyhow::Result<Option<OwnedBlob>> {
        let block_key = block_key(id);
        let t = self.root.try_get(&block_key)?;
        let t = t.map(|x| x.load(&self.store)).transpose()?;
        Ok(t)
    }

    fn has_block(&self, id: &Cid) -> anyhow::Result<bool> {
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

    fn links(&self, id: &Cid) -> anyhow::Result<Option<impl Iterator<Item = Cid>>> {
        let key = block_key(id);
        Ok(if let Some(blob) = self.root.try_get(&key)? {
            let blob = blob.load(&self.store)?;
            let mut t = Vec::new();
            get_links(id, &blob, &mut t)?;
            Some(t.into_iter())
        } else {
            None
        })
    }

    fn dump(&self) -> anyhow::Result<()> {
        todo!()
        // self.root
        //     .dump_tree_custom("", &self.store, format_prefix, format_value, |_| {
        //         Ok("".into())
        //     })
    }

    fn put(&mut self, block: &Block) -> anyhow::Result<()> {
        let block_key = block_key(&block.cid());
        self.root.insert(&block_key, block.data())?;
        Ok(())
    }

    fn get(&mut self, cid: &Cid) -> anyhow::Result<Option<OwnedBlob>> {
        self.get_block(cid)
    }

    fn has(&mut self, cid: &Cid) -> anyhow::Result<bool> {
        self.has_block(cid)
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let file = tempfile()?;
    let store = PagedFileStore::<4194304>::new(file)?;
    let mut ipfs = Ipfs::new(Arc::new(store.clone()))?;
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
        ipfs.put(&block)?;
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
        res += ipfs.get(hash)?.unwrap().len() as u64;
    }
    println!("done {} {}", res, t0.elapsed().as_secs_f64());
    println!("traversing all (in order, hot)");
    let t0 = Instant::now();
    let mut res = 0u64;
    for hash in &hashes {
        res += ipfs.get(hash)?.unwrap().len() as u64;
    }
    println!("done {} {}", res, t0.elapsed().as_secs_f64());
    hashes.sort();
    for _ in 0..10 {
        println!("traversing all (random)");
        let t0 = Instant::now();
        let mut res = 0u64;
        for hash in &hashes {
            res += ipfs.get(hash)?.unwrap().len() as u64;
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
