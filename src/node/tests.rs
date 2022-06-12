use obey::{binary_element_test, binary_property_test, TestSamples};
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use crate::store::{MemStore, MemStore2};

use super::*;

// fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
//     proptest::strategy::Union::new_weighted(vec![
//         (10, proptest::collection::vec(b'0'..b'9', 0..9)),
//         (1, proptest::collection::vec(b'0'..b'9', 128..129)),
//     ])
// }

// fn arb_value() -> impl Strategy<Value = Vec<u8>> {
//     proptest::strategy::Union::new_weighted(vec![
//         (10, proptest::collection::vec(any::<u8>(), 0..9)),
//         (1, proptest::collection::vec(any::<u8>(), 128..129)),
//     ])
// }

fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
    proptest::strategy::Union::new_weighted(vec![
        (10, proptest::collection::vec(b'0'..b'9', 0..9)),
        (1, proptest::collection::vec(b'0'..b'9', 128..129)),
    ])
    // proptest::collection::vec(b'0'..b'9', 0..9)
}

fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    // proptest::collection::vec(any::<u8>(), 0..2)
    proptest::strategy::Union::new_weighted(vec![
        (10, proptest::collection::vec(any::<u8>(), 0..9)),
        (1, proptest::collection::vec(any::<u8>(), 128..129)),
    ])
}

fn arb_tree_contents() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    proptest::collection::btree_map(arb_prefix(), arb_value(), 0..10)
}

fn arb_owned_tree() -> impl Strategy<Value = Tree> {
    arb_tree_contents().prop_map(|x| mk_owned_tree(&x))
}

fn mk_owned_tree(v: &BTreeMap<Vec<u8>, Vec<u8>>) -> Tree {
    v.clone().into_iter().collect()
}

#[test]
fn sizes2() {
    assert_eq!(
        std::mem::size_of::<OwnedTreeNode<NoStore>>(),
        4 * std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of::<BorrowedTreeNode<NoStore>>(),
        4 * std::mem::size_of::<usize>()
    );
    // todo: get this to 4xusize with some union magic?
    assert_eq!(
        std::mem::size_of::<TreeNodeRef<NoStore>>(),
        5 * std::mem::size_of::<usize>()
    );
    println!("{}", std::mem::size_of::<OwnedTreeNode<NoStore>>());
    println!("{}", std::mem::size_of::<BorrowedTreeNode<NoStore>>());
    println!("{}", std::mem::size_of::<TreeNodeRef<NoStore>>());
}

#[test]
fn new_build_bench() {
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
        .collect::<Vec<_>>();
    let elems2 = elems.clone();
    let elems3 = elems.clone();
    let elems_bt = elems.iter().cloned().collect::<BTreeMap<_, _>>();
    let mut t = OwnedTreeNode::<NoStore>::EMPTY;
    let t0 = Instant::now();
    for (k, v) in elems.clone() {
        let b = OwnedTreeNode::<NoStore>::single(&k, &v);
        outer_combine_with(
            &mut t,
            NoStore,
            &b.as_ref(),
            NoStore,
            DowncastConverter,
            |_, _| Ok(()),
        )
        .unwrap();
    }
    println!("build {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    let r: BTreeMap<_, _> = elems2.into_iter().collect();
    println!("build ref {:#?}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        assert!(t.contains_key(&key, &NoStore).unwrap());
    }
    println!("validate contains_key {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        assert!(elems_bt.contains_key(key));
    }
    println!("validate contains_key ref {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: OwnedValue<NoStore> = t.get(&key, &NoStore).unwrap().unwrap();
        assert_eq!(v.read().unwrap(), &value[..]);
    }
    println!("validate get {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v = elems_bt.get(key).unwrap();
        assert_eq!(v, value);
    }
    println!("validate get ref {}", t0.elapsed().as_secs_f64());

    let store = MemStore2::default();
    let mut target = Vec::new();
    let t0 = Instant::now();
    // t.dump(0, &NoStore).unwrap();
    t.serialize(&mut target, &store).unwrap();
    println!("{} {}", Hex::new(&target), t0.elapsed().as_secs_f64());
    let d = OwnedTreeNode::<MemStore2>::deserialize(&target).unwrap();
    println!("{:?}", d);
    // d.dump(0, &store).unwrap();

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: OwnedValue<MemStore2> = d.get(&key, &store).unwrap().unwrap();
        assert_eq!(v.read().unwrap(), &value[..]);
    }
    println!("validate get attached {}", t0.elapsed().as_secs_f64());
}

#[test]
fn new_smoke() {
    {
        let a = OwnedTreeNode::single(b"a", b"1");
        let b = OwnedTreeNode::single(b"b", b"2");
        println!("a={:?}", a);
        println!("b={:?}", b);
        let mut r = a;
        outer_combine_with(
            &mut r,
            NoStore,
            &b.as_ref(),
            NoStore,
            DowncastConverter,
            |a, b| Ok(()),
        )
        .unwrap();
        println!("r={:?}", r);
    }

    {
        let a = OwnedTreeNode::<NoStore>::single(b"aa", b"1");
        let b = OwnedTreeNode::<NoStore>::single(b"ab", b"2");
        println!("a={:?}", a);
        println!("b={:?}", b);
        let mut r = a;
        outer_combine_with(
            &mut r,
            NoStore,
            &b.as_ref(),
            NoStore,
            DowncastConverter,
            |a, b| Ok(()),
        )
        .unwrap();
        println!("r={:?}", r);
    }
}
