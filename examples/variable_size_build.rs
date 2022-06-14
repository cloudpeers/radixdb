use std::{collections::BTreeMap, time::Instant};

// use radixdb::VSRadixTree;

fn main() {
    let elems = (0..2000_000u64)
        .map(|i| {
            if i % 100000 == 0 {
                println!("{}", i);
            }
            (
                i.to_string().as_bytes().to_vec(),
                i.to_string().as_bytes().to_vec(),
            )
        })
        .collect::<Vec<(_, _)>>();
    let t0 = Instant::now();
    println!("building tree");
    // let tree: VSRadixTree = elems.into_iter().collect();
    println!("unattached tree {} s", t0.elapsed().as_secs_f64());
}
