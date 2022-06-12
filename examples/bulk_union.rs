use std::{collections::BTreeMap, time::Instant};

use log::info;
// use radixdb::VSRadixTree;

// fn do_test() -> anyhow::Result<()> {
//     let n1 = 2000000u64;
//     let n2 = 100000u64;
//     let elems0 = (0..n1)
//         .map(|i| {
//             if i % 100000 == 0 {
//                 info!("{}", i);
//             }
//             (
//                 i.to_string().as_bytes().to_vec(),
//                 i.to_string().as_bytes().to_vec(),
//             )
//         })
//         .collect::<BTreeMap<_, _>>();

//     let elems1 = (n1..n1 + n2)
//         .map(|i| {
//             if i % 100000 == 0 {
//                 info!("{}", i);
//             }
//             (
//                 i.to_string().as_bytes().to_vec(),
//                 i.to_string().as_bytes().to_vec(),
//             )
//         })
//         .collect::<BTreeMap<_, _>>();
//     let tree0: VSRadixTree = elems0.clone().into_iter().collect();
//     let tree1: VSRadixTree = elems1.clone().into_iter().collect();
//     for _ in 0..100 {
//         let mut res = tree0.clone();
//         let t0 = Instant::now();
//         res.outer_combine_with(&tree1, |a, b| Some(b.to_owned()));
//         info!("bulk union {} {} {} s", n1, n2, t0.elapsed().as_secs_f64());
//     }

//     for _ in 0..100 {
//         let mut res = elems0.clone();
//         let elems1 = elems1.clone();
//         let t0 = Instant::now();
//         for (k, v) in elems1 {
//             res.insert(k, v);
//         }
//         info!(
//             "BTreeMap union {} {} {} s",
//             n1,
//             n2,
//             t0.elapsed().as_secs_f64()
//         );
//     }

//     return Ok(());
// }

// fn init_logger() {
//     let _ = env_logger::builder()
//         // Include all events in tests
//         .filter_level(log::LevelFilter::max())
//         // Ensure events are captured by `cargo test`
//         .is_test(true)
//         // Ignore errors initializing the logger if tests race to configure it
//         .try_init();
// }

// fn browser_compare() -> anyhow::Result<()> {
//     init_logger();
//     do_test()
// }

fn main() {
    // browser_compare().unwrap()
}
