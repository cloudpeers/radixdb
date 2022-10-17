use std::{sync::{atomic::AtomicU64, Arc}, path::Path};

use marble::Marble;
use radixdb::{RadixTree, store::{BlobStore, Blob}};
use anyhow::Context;

#[derive(Debug)]
struct MarbleStoreInner {
    marble: Marble,
    next_id: AtomicU64,
}

#[derive(Debug, Clone)]
struct MarbleStore(Arc<MarbleStoreInner>);

impl MarbleStore {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let marble = marble::open(&path)?;
        let (next_id, _) = marble.free_object_ids();
        Ok(Self(Arc::new(MarbleStoreInner {
            marble,
            next_id: AtomicU64::new(next_id),
        })))
    }
}

impl BlobStore for MarbleStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<Blob<'static>, Self::Error> {
        let id = u64::from_be_bytes(id.try_into()?);
        let maybe_data = self.0.marble.read(id)?;
        let data = maybe_data.context("not found")?;
        let arc: Arc<Vec<u8>> = Arc::new(data.into());
        Ok(Blob::from(arc))
    }

    fn write(&self, data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        let id = self.0.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.0.marble.write_batch(vec![(id, Some(data))])?;
        self.0.marble.sync_all()?;
        let n = self.0.marble.maintenance()?;
        println!("maintenance {}", n);
        Ok(id.to_be_bytes().to_vec())
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        self.0.marble.sync_all()?;
        Ok(())
    }
}

const LOW_NAMES: &[&str] = &[
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
];
const TENS_NAMES: &[&str] = &[
    "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
];
const BIG_NAMES: &[&str] = &["thousand", "million", "billion"];

/**
 * Converts an integer number into words (american english).
 * @author Christian d'Heureuse, Inventec Informatik AG, Switzerland, www.source-code.biz
 */
fn number_to_words(n0: i64) -> String {
    let mut n = n0;
    if n < 0 {
        "minus ".to_string() + &number_to_words(-n)
    } else if n <= 999 {
        convert999(n)
    } else {
        let mut s: String = "".to_string();
        let mut t: usize = 0;
        while n > 0 {
            if n % 1000 != 0 {
                let mut s2 = convert999(n % 1000);
                if t > 0 {
                    s2 = s2 + " " + BIG_NAMES[t - 1];
                }
                if s.is_empty() {
                    s = s2
                } else {
                    s = s2 + ", " + &s;
                }
            }
            n /= 1000;
            t += 1;
        }
        s
    }
}

fn convert999(n: i64) -> String {
    let s1 = LOW_NAMES[(n / 100) as usize].to_string() + " hundred";
    let s2 = convert99(n % 100);
    if n <= 99 {
        s2
    } else if n % 100 == 0 {
        s1
    } else {
        s1 + " " + &s2
    }
}

fn convert99(n: i64) -> String {
    if n < 20 {
        LOW_NAMES[n as usize].to_string()
    } else {
        let s = TENS_NAMES[(n / 10 - 2) as usize].to_string();
        if n % 10 == 0 {
            s
        } else {
            s + "-" + LOW_NAMES[(n % 10) as usize]
        }
    }
}

fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    // write a large tree into a temp file
    let store = MarbleStore::new("test")?;
    let mut tree = RadixTree::empty(store.clone());
    for i in 0..1_000_000 {
        let key = number_to_words(i);
        let value = i.to_string().repeat(10000);
        tree.try_insert(key, value)?;
        if i % 100000 == 0 {
            tree.try_reattach()?;
            println!("flush! {:?}", store);
        }
    }
    let final_id = tree.try_reattach()?;
    let final_size = u64::from_be_bytes(final_id[0..8].try_into()?);
    for e in tree.try_iter() {
        let (k, v) = e?;
        let v = v.load(&store)?;
        println!("{:?} {:?}", std::str::from_utf8(k.as_ref())?, v.len());
    }
    println!("final size {}", final_size);
    //     let store = PagedFileStore::new()?;
    Ok(())
}
