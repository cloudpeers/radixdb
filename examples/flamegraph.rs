use std::{collections::BTreeMap, fs, sync::Arc, time::Instant};

use log::info;
use radixdb::{
    store::{DynBlobStore, DynBlobStore2, PagedFileStore},
    VSRadixTree,
};
use tempfile::tempdir;

fn do_test(store: DynBlobStore2) -> anyhow::Result<()> {
    let elems = (0..2000_000u64)
        .map(|i| {
            if i % 100000 == 0 {
                info!("{}", i);
            }
            (
                i.to_string().as_bytes().to_vec(),
                i.to_string().as_bytes().to_vec(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let t0 = Instant::now();
    info!("building tree");
    let tree: VSRadixTree = elems.clone().into_iter().collect();
    info!("unattached tree {} s", t0.elapsed().as_secs_f64());

    // let x = elems.clone();
    // let t0 = Instant::now();
    // info!("building tree reference");
    // let _: BTreeMap<_, _> = x.into_iter().collect();
    // info!("BTreeMap {} s", t0.elapsed().as_secs_f64());
    // info!("traversing unattached tree...");
    // let t0 = Instant::now();
    // let mut n = 0;
    // for _ in tree.iter() {
    //     n += 1;
    // }
    // info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());

    // info!("traversing elements...");
    // let tree: Tree = elems.clone().into_iter().collect();
    // let t0 = Instant::now();
    // let mut n = 0;
    // for _ in elems.iter() {
    //     n += 1;
    // }
    // info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());

    info!("getting all elements ref");
    let t0 = Instant::now();
    let mut n = 0;
    for key in elems.keys() {
        if elems.get(key).is_some() {
            n += 1;
        }
    }
    info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());

    for _ in 0..100 {
        info!("getting all elements");
        let t0 = Instant::now();
        let mut n = 0;
        for key in elems.keys() {
            if tree.get(key.as_ref()).is_some() {
                n += 1;
            }
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    }

    for _ in 0..1 {
        info!("traversing unattached tree values...");
        let t0 = Instant::now();
        let mut n = 0;
        for _ in tree.values() {
            n += 1;
        }
        info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    }

    info!("attaching tree...");
    let t0 = Instant::now();
    // let tree = tree.try_attached(store)?;
    // store.sync()?;
    info!("attached tree {} s", t0.elapsed().as_secs_f32());
    info!("traversing attached tree values...");
    let t0 = Instant::now();
    let mut n = 0;
    for item in tree.try_values() {
        if item.is_err() {
            info!("{:?}", item);
        }
        n += 1;
    }
    info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    info!("traversing attached tree...");
    let t0 = Instant::now();
    let mut n = 0;
    for _ in tree.try_iter() {
        n += 1;
    }
    info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    info!("detaching tree...");
    let t0 = Instant::now();
    // let tree = tree.detached()?;
    info!("detached tree {} s", t0.elapsed().as_secs_f32());
    info!("traversing unattached tree...");
    let t0 = Instant::now();
    let mut n = 0;
    for _ in tree.iter() {
        n += 1;
    }
    info!("done {} items, {} s", n, t0.elapsed().as_secs_f32());
    Ok(())
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn browser_compare() -> anyhow::Result<()> {
    init_logger();
    let dir = tempdir()?;
    let path = dir.path().join("large2.rdb");
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let db = PagedFileStore::<1048576>::new(file).unwrap();
    let store: DynBlobStore2 = Arc::new(db);
    do_test(store)
}

fn main() {
    browser_compare().unwrap()
}
