use obey::{binary_element_test, binary_property_test, TestSamples};
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use crate::store::MemStore;

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

fn to_btree_map(t: &Tree) -> BTreeMap<Vec<u8>, Vec<u8>> {
    t.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
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

    let store = MemStore::default();
    let mut target = Vec::new();
    let t0 = Instant::now();
    // t.dump(0, &NoStore).unwrap();
    t.serialize(&mut target, &store).unwrap();
    println!("{} {}", Hex::new(&target), t0.elapsed().as_secs_f64());
    let d = OwnedTreeNode::<MemStore>::deserialize(&target).unwrap();
    println!("{:?}", d);
    // d.dump(0, &store).unwrap();

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: OwnedValue<MemStore> = d.get(&key, &store).unwrap().unwrap();
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

proptest! {

    #[test]
    fn btreemap_tree_roundtrip(x in arb_tree_contents()) {
        let reference = x;
        let tree = mk_owned_tree(&reference);
        let actual = to_btree_map(&tree);
        prop_assert_eq!(reference, actual);
    }

    #[test]
    fn get_contains(x in arb_tree_contents()) {
        let reference = x;
        let tree = mk_owned_tree(&reference);
        for (k, v) in reference {
            prop_assert!(tree.contains_key(&k));
            prop_assert_eq!(tree.get(&k).map(|x| x.to_vec()), Some(v));
        }
    }

    #[test]
    fn scan_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
        let reference = x;
        let tree = mk_owned_tree(&reference);
        let filtered = tree.scan_prefix(&prefix);
        for (k, v) in filtered {
            prop_assert!(k.as_ref().starts_with(&prefix));
            let t = reference.get(k.as_ref()).unwrap();
            let v = v.as_ref();
            prop_assert_eq!(v, t);
        }
    }
}
