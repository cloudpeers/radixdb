//!
//! A radix tree that implements a content-addressed store for ipfs
//!
//!
use radixdb::{
    node::{DowncastConverter, OwnedSlice, TreePrefix, TreeValue},
    store::{Blob, BlobStore, DynBlobStore, PagedFileStore},
    *,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use tempfile::tempfile;

/// alias -> id
const ALIAS: &[u8] = "alias".as_bytes();
/// the actual block data
const BLOCK: &[u8] = "block".as_bytes();
/// links of a block
const LINKS: &[u8] = "links".as_bytes();
/// id -> cid
///
/// cids<id> => cid
const CIDS: &[u8] = "cids".as_bytes();
/// cid -> id
///
/// ids<hash> => id
const IDS: &[u8] = "ids".as_bytes();

fn format_prefix(prefix: &[u8]) -> anyhow::Result<String> {
    Ok(if prefix.starts_with(IDS) {
        format!("\"ids\" {}", hex::encode(&prefix[IDS.len()..]))
    } else if prefix.starts_with(CIDS) {
        format!("\"cids\"  {}", hex::encode(&prefix[CIDS.len()..]))
    } else if prefix.starts_with(ALIAS) {
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

fn id_key(hash: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(IDS.len() + hash.len());
    res.extend_from_slice(IDS);
    res.extend_from_slice(hash);
    res
}

fn alias_key(alias: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(ALIAS.len() + alias.len());
    res.extend_from_slice(ALIAS);
    res.extend_from_slice(alias);
    res
}

fn block_key(id: u64) -> Vec<u8> {
    let mut res = Vec::with_capacity(BLOCK.len() + 8);
    res.extend_from_slice(BLOCK);
    res.extend_from_slice(&id.to_be_bytes());
    res
}

fn links_key(id: u64) -> Vec<u8> {
    let mut res = Vec::with_capacity(BLOCK.len() + 8 + LINKS.len());
    res.extend_from_slice(BLOCK);
    res.extend_from_slice(&id.to_be_bytes());
    res.extend_from_slice(LINKS);
    res
}

fn cid_key(id: u64) -> Vec<u8> {
    let mut res = Vec::with_capacity(CIDS.len() + 8);
    res.extend_from_slice(CIDS);
    res.extend_from_slice(&id.to_be_bytes());
    res
}

fn serialize_links(links: &BTreeSet<u64>) -> Vec<u8> {
    let mut res = Vec::with_capacity(links.len());
    for link in links {
        res.extend_from_slice(&link.to_be_bytes());
    }
    res
}

fn links_iter(blob: &[u8]) -> anyhow::Result<impl Iterator<Item = u64>> {
    anyhow::ensure!(blob.len() % 8 == 0);
    // todo: avoid going via a vec
    let ids = blob
        .as_ref()
        .chunks_exact(8)
        .map(|chunk| u64::from_be_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    Ok(ids.into_iter())
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
            Ok(*a = b.downcast())
        })
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), S::Error> {
        self.try_left_combine_with(
            &RadixTree::single(key, vec![]),
            DowncastConverter,
            |a, b| Ok(*a = TreeValue::none()),
        )
    }
}

struct Block {
    hash: Vec<u8>,
    data: Vec<u8>,
    links: Vec<Vec<u8>>,
}

impl Block {
    fn new(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize().to_vec();
        Self {
            hash,
            data: data.to_vec(),
            links: Vec::new(),
        }
    }
}

impl Ipfs {
    /// compute the set of all live ids
    fn live_set(&self) -> anyhow::Result<BTreeSet<u64>> {
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
                if let Some(children) = self.links(*parent)? {
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
        for id in live_set {
            dead.remove(&id);
        }
        println!("{}", t0.elapsed().as_secs_f64());
        let t0 = Instant::now();
        println!("taking out the dead");
        for id in dead {
            if let Some(cid) = self.get_cid(id)? {
                self.root.remove(&id_key(&cid))?;
            }
            self.root.remove(&links_key(id))?;
            self.root.remove(&block_key(id))?;
            self.root.remove(&cid_key(id))?;
        }
        println!("{}", t0.elapsed().as_secs_f64());
        Ok(())
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        todo!()
        // self.root.attach(&mut self.store)?;
        // self.root.store.sync()
    }

    fn aliases(&self) -> anyhow::Result<BTreeSet<u64>> {
        self.root
            .try_scan_prefix(ALIAS)?
            .map(|r| {
                r.and_then(|(_, v)| {
                    let blob = v.load(&self.store)?.unwrap();
                    Ok(u64::from_be_bytes(blob.as_ref().try_into()?))
                })
            })
            .collect()
    }

    /// all ids we know of
    fn ids(&self) -> anyhow::Result<BTreeSet<u64>> {
        self.root
            .try_scan_prefix(IDS)?
            .map(|r| {
                r.and_then(|(_, v)| {
                    let blob = v.load(&self.store)?.unwrap();
                    Ok(u64::from_be_bytes(blob.as_ref().try_into()?))
                })
            })
            .collect()
    }

    fn new(store: DynBlobStore) -> anyhow::Result<Self> {
        Ok(Self {
            root: RadixTree::new(store.clone()),
            store,
        })
    }

    fn get_id(&self, hash: &[u8]) -> anyhow::Result<Option<u64>> {
        let id_key = id_key(hash);
        Ok(if let Some(blob) = self.root.try_get(&id_key)? {
            let id: [u8; 8] = blob.as_ref().try_into()?;
            Some(u64::from_be_bytes(id))
        } else {
            None
        })
    }

    fn get_cid(&self, id: u64) -> anyhow::Result<Option<OwnedSlice<u8>>> {
        let id_key = cid_key(id);
        self.root.try_get(&id_key)
    }

    fn get_block(&self, id: u64) -> anyhow::Result<Option<OwnedSlice<u8>>> {
        let block_key = block_key(id);
        self.root.try_get(&block_key)
    }

    fn alias(&mut self, name: &[u8], hash: Option<&[u8]>) -> anyhow::Result<()> {
        if let Some(hash) = hash {
            let id = self.get_or_create_id(hash)?;
            self.root.insert(&alias_key(name), &id.to_be_bytes())?;
        } else {
            self.root.remove(&alias_key(name))?;
        }
        Ok(())
    }

    fn links(&self, id: u64) -> anyhow::Result<Option<impl Iterator<Item = u64>>> {
        Ok(if let Some(blob) = self.root.try_get(&links_key(id))? {
            Some(links_iter(blob.as_ref())?)
        } else {
            None
        })
    }

    fn get_or_create_id(&mut self, hash: &[u8]) -> anyhow::Result<u64> {
        let id_key = id_key(hash);
        Ok(if let Some(blob) = self.root.try_get(&id_key)? {
            let id: [u8; 8] = blob.as_ref().try_into()?;
            u64::from_be_bytes(id)
        } else {
            let cids = self.root.try_filter_prefix(CIDS)?;
            // compute higest id
            let id = if let Some((prefix, _)) = cids.try_last_entry(TreePrefix::default())? {
                // get id from prefix and inc by one
                let prefix = prefix.load(&self.store)?;
                let id: [u8; 8] = prefix[prefix.len() - 8..].try_into()?;
                u64::from_be_bytes(id) + 1
            } else {
                0
            };
            let cid_key = cid_key(id);
            self.root.insert(&id_key, &u64::to_be_bytes(id))?;
            self.root.insert(&cid_key, &hash)?;
            id
        })
    }

    fn dump(&self) -> anyhow::Result<()> {
        todo!()
        // self.root
        //     .dump_tree_custom("", &self.store, format_prefix, format_value, |_| {
        //         Ok("".into())
        //     })
    }

    fn put(&mut self, block: &Block) -> anyhow::Result<u64> {
        let id = self.get_or_create_id(&block.hash)?;
        let block_key = block_key(id);
        let link_ids = block
            .links
            .iter()
            .map(|link| self.get_or_create_id(link))
            .collect::<anyhow::Result<BTreeSet<_>>>()?;
        self.root.insert(&block_key, &block.data)?;
        self.root
            .insert(&links_key(id), &serialize_links(&link_ids))?;
        Ok(id)
    }
    fn get(&mut self, hash: &[u8]) -> anyhow::Result<Option<OwnedSlice<u8>>> {
        Ok(if let Some(id) = self.get_id(hash)? {
            self.get_block(id)?
        } else {
            None
        })
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let file = tempfile()?;
    let store = PagedFileStore::<4194304>::new(file)?;
    let mut ipfs = Ipfs::new(Arc::new(store.clone()))?;
    let mut hashes = Vec::new();
    for i in 0..1000_000u64 {
        println!("putting block {}", i);
        let mut data = [0u8; 10000];
        data[0..8].copy_from_slice(&i.to_be_bytes());
        let block = Block::new(&data);
        ipfs.put(&block)?;
        hashes.push(block.hash);
    }
    println!("committing {:?}", store);
    ipfs.commit()?;
    println!("done {:?}", store);
    // ipfs.dump()?;
    let id1 = ipfs.get_or_create_id(b"abcd")?;
    let id2 = ipfs.get_or_create_id(b"abcd")?;
    let id3 = ipfs.get_or_create_id(b"abce")?;
    ipfs.put(&Block {
        hash: b"abcd".to_vec(),
        data: b"SOMEDATAFINALLY".to_vec(),
        links: vec![b"abce".to_vec(), b"abcf".to_vec(), b"abcg".to_vec()],
    })?;
    let data = ipfs.get(b"abcd")?;
    ipfs.alias(b"root1", Some(b"abcd"))?;
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
    println!("traversing all (random)");
    let t0 = Instant::now();
    let mut res = 0u64;
    for hash in &hashes {
        res += ipfs.get(hash)?.unwrap().len() as u64;
    }
    println!("done {} {}", res, t0.elapsed().as_secs_f64());
    // ipfs.dump()?;
    println!("performing gc");
    ipfs.gc()?;
    println!("committing {:?}", store);
    ipfs.commit()?;
    println!("done {:?}", store);
    // ipfs.dump()?;
    Ok(())
}
