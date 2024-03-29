#![allow(clippy::redundant_clone)]
use crate::store::MemStore;
use obey::{binary_element_test, binary_property_test, TestSamples};
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use super::*;

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

fn arb_owned_tree() -> impl Strategy<Value = RadixTree> {
    arb_tree_contents().prop_map(|x| mk_owned_tree(&x))
}

fn mk_owned_tree(v: &BTreeMap<Vec<u8>, Vec<u8>>) -> RadixTree {
    v.clone().iter().collect()
}

fn to_btree_map(t: &RadixTree) -> BTreeMap<Vec<u8>, Vec<u8>> {
    t.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

#[test]
fn sizes2() {
    assert_eq!(
        std::mem::size_of::<TreeNode<Detached>>(),
        4 * std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of::<BorrowedTreeNode<Detached>>(),
        4 * std::mem::size_of::<usize>()
    );
    // todo: get this to 4xusize with some union magic?
    assert_eq!(
        std::mem::size_of::<TreeNodeRef<Detached>>(),
        5 * std::mem::size_of::<usize>()
    );
    println!("{}", std::mem::size_of::<TreeNode<Detached>>());
    println!("{}", std::mem::size_of::<BorrowedTreeNode<Detached>>());
    println!("{}", std::mem::size_of::<TreeNodeRef<Detached>>());
}

// #[test]
// fn macro_test_blob() {
//     // from https://en.wikipedia.org/wiki/Radix_tree
//     let tree = radixtree! {
//         b"romane" => b"",
//         b"romanus" => b"",
//         b"romulus" => b"",
//         b"rubens" => b"",
//         b"ruber" => b"",
//         b"rubicon" => b"",
//         b"rubicundus" => b"",
//     };
//     let elems: Vec<(&[u8], &[u8])> = vec![
//         (b"romane", b""),
//         (b"romanus", b""),
//         (b"romulus", b""),
//         (b"rubens", b""),
//         (b"ruber", b""),
//         (b"rubicon", b""),
//         (b"rubicundus", b""),
//     ];
//     let tree2 = elems.into_iter().collect::<RadixTree>();
//     assert_eq!(tree, tree2);
//     assert!(tree.contains_key(b"romane"));
// }
//
// #[test]
// fn macro_test_str() {
//     // from https://en.wikipedia.org/wiki/Radix_tree
//     let tree = radixtree! {
//         "romane" => "",
//         "romanus" => "",
//         "romulus" => "",
//         "rubens" => "",
//         "ruber" => "",
//         "rubicon" => "",
//         "rubicundus" => "",
//     };
//     let elems: Vec<(&str, &str)> = vec![
//         ("romane", ""),
//         ("romanus", ""),
//         ("romulus", ""),
//         ("rubens", ""),
//         ("ruber", ""),
//         ("rubicon", ""),
//         ("rubicundus", ""),
//     ];
//     let tree2 = elems.into_iter().collect::<RadixTree>();
//     assert_eq!(tree, tree2);
//     assert!(tree.contains_key("romane"));
// }

#[test]
fn new_build_bench() {
    let elems = (0..2_000_000u64)
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
    let mut t = TreeNode::<Detached>::EMPTY;
    let t0 = Instant::now();
    for (k, v) in elems.clone() {
        let b = TreeNode::<Detached>::single(&k, &v);
        outer_combine_with(
            &mut t,
            Detached,
            &b.as_ref(),
            Detached,
            DowncastConverter,
            |_, _| Ok(()),
        )
        .unwrap();
    }
    println!("build {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    let _r: BTreeMap<_, _> = elems2.into_iter().collect();
    println!("build ref {:#?}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, _value) in &elems3 {
        assert!(t.contains_key(key, &Detached).unwrap());
    }
    println!("validate contains_key {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, _value) in &elems3 {
        assert!(elems_bt.contains_key(key));
    }
    println!("validate contains_key ref {}", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: Value = t.get(key, &Detached).unwrap().unwrap();
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
    let d = TreeNode::<MemStore>::deserialize(&target).unwrap();
    println!("{:?}", d);
    // d.dump(0, &store).unwrap();

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: Value<MemStore> = d.get(key, &store).unwrap().unwrap();
        assert_eq!(v.read().unwrap(), &value[..]);
    }
    println!("validate get attached {}", t0.elapsed().as_secs_f64());
}

#[test]
fn new_smoke() {
    {
        let a = TreeNode::single(b"a", b"1");
        let b = TreeNode::single(b"b", b"2");
        println!("a={:?}", a);
        println!("b={:?}", b);
        let mut r = a;
        outer_combine_with(
            &mut r,
            Detached,
            &b.as_ref(),
            Detached,
            DowncastConverter,
            |_, _| Ok(()),
        )
        .unwrap();
        println!("r={:?}", r);
    }

    {
        let a = TreeNode::<Detached>::single(b"aa", b"1");
        let b = TreeNode::<Detached>::single(b"ab", b"2");
        println!("a={:?}", a);
        println!("b={:?}", b);
        let mut r = a;
        outer_combine_with(
            &mut r,
            Detached,
            &b.as_ref(),
            Detached,
            DowncastConverter,
            |_, _| Ok(()),
        )
        .unwrap();
        println!("r={:?}", r);
    }
}

impl TestSamples<Vec<u8>, Option<Vec<u8>>> for RadixTree {
    fn samples(&self, res: &mut BTreeSet<Vec<u8>>) {
        res.insert(vec![]);
        for (k, _) in self.iter() {
            let a = k.as_ref().to_vec();
            let mut b = a.clone();
            let mut c = a.clone();
            b.push(0);
            c.pop();
            res.insert(a);
            res.insert(b);
            res.insert(c);
        }
    }

    fn at(&self, elem: Vec<u8>) -> Option<Vec<u8>> {
        self.get(&elem).map(|x| x.to_vec())
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
    fn attach_detach_roundtrip(x in arb_tree_contents()) {
        let reference = x;
        let tree = mk_owned_tree(&reference);
        let store = MemStore::default();
        let tree = tree.try_attached(store.clone()).unwrap();
        let actual = tree.try_iter().map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.load(&store).unwrap().to_vec())
        }).collect();
        prop_assert_eq!(&reference, &actual);

        let tree = tree.try_detached().unwrap();
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


    #[test]
    fn filter_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
        let reference = x;
        let tree = mk_owned_tree(&reference);
        let filtered = tree.filter_prefix(&prefix, &prefix);
        for (k, v) in filtered.iter() {
            prop_assert!(k.as_ref().starts_with(&prefix));
            let t = reference.get(k.as_ref()).unwrap();
            prop_assert_eq!(v.as_ref(), t);
        }
    }

    #[test]
    fn union(a in arb_tree_contents(), b in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        // check right biased union
        let rbut = at.outer_combine(&bt, |_, b| Some(b.to_owned()));
        let rbu = to_btree_map(&rbut);
        let mut rbu_reference = a.clone();
        for (k, v) in b.clone() {
            rbu_reference.insert(k, v);
        }
        prop_assert_eq!(rbu, rbu_reference);
        // check left biased union
        let lbut = at.outer_combine(&bt, |a, _| Some(a.to_owned()));
        let lbu = to_btree_map(&lbut);
        let mut lbu_reference = b.clone();
        for (k, v) in a.clone() {
            lbu_reference.insert(k, v);
        }
        prop_assert_eq!(lbu, lbu_reference);
    }

    #[test]
    fn union_sample(a in arb_owned_tree(), b in arb_owned_tree()) {
        let r = a.outer_combine(&b, |a, _| Some(a.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, b| a.or(b)));

        let r = a.outer_combine(&b, |_, b| Some(b.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, b| b.or(a)));
    }

    #[test]
    fn union_with(a in arb_owned_tree(), b in arb_owned_tree()) {
        // check right biased union
        let r1 = a.outer_combine(&b, |_, b| Some(b.to_owned()));
        let mut r2 = a.clone();
        r2.outer_combine_with(&b, |a, b| a.set(Some(b)));
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
        // check left biased union
        let r1 = a.outer_combine(&b, |a, _| Some(a.to_owned()));
        let mut r2 = a.clone();
        r2.outer_combine_with(&b, |_, _| {});
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
    }

    #[test]
    fn intersection(a in arb_tree_contents(), b in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        // check right biased intersection
        let rbut = at.inner_combine(&bt, |_, b| Some(b.to_owned()));
        let rbu = to_btree_map(&rbut);
        let mut rbu_reference = b.clone();
        rbu_reference.retain(|k, _| a.contains_key(k));
        prop_assert_eq!(rbu, rbu_reference);
        // check left biased intersection
        let lbut = at.inner_combine(&bt, |a, _| Some(a.to_owned()));
        let lbu = to_btree_map(&lbut);
        let mut lbu_reference = a.clone();
        lbu_reference.retain(|k, _| b.contains_key(k));
        prop_assert_eq!(lbu, lbu_reference);
    }

    #[test]
    fn intersection_sample(a in arb_owned_tree(), b in arb_owned_tree()) {
        let r = a.inner_combine(&b, |a, _| Some(a.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, b| b.and(a)));

        let r = a.inner_combine(&b, |_, b| Some(b.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, b| a.and(b)));
    }

    #[test]
    fn intersection_with(a in arb_owned_tree(), b in arb_owned_tree()) {
        // right biased intersection
        let r1 = a.inner_combine(&b, |_, b| Some(b.to_owned()));
        let mut r2 = a.clone();
        r2.inner_combine_with(&b, |a, b| a.set(Some(b)));
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
        // left biased intersection
        let r1 = a.inner_combine(&b, |a, _| Some(a.to_owned()));
        let mut r2 = a;
        r2.inner_combine_with(&b, |_, _| {});
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
    }

    #[test]
    fn difference(a in arb_tree_contents(), b in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        // replace all values that are common with the b side
        let rbut = at.left_combine(&bt, |_, b| Some(b.to_owned()));
        let rbu = to_btree_map(&rbut);
        let mut rbu_reference = a.clone();
        for (k, v) in b.clone() {
            if rbu_reference.contains_key(&k) {
                rbu_reference.insert(k, v);
            }
        }
        prop_assert_eq!(rbu, rbu_reference);
        // compute the actual difference
        let lbut = at.left_combine(&bt, |_, _| None);
        let lbu = to_btree_map(&lbut);
        let mut lbu_reference = a.clone();
        lbu_reference.retain(|k, _| !b.contains_key(k));
        prop_assert_eq!(lbu, lbu_reference);
    }

    #[test]
    fn difference_sample(a in arb_owned_tree(), b in arb_owned_tree()) {
        let r = a.left_combine(&b, |a, _| Some(a.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, _| a));

        let r = a.left_combine(&b, |_, b| Some(b.to_owned()));
        let right = |a: Option<Vec<u8>>, b: Option<Vec<u8>>| {
            if a.is_some() && b.is_some() { b } else { a }
        };
        prop_assert!(binary_element_test(&a, &b, r, right));
    }

    #[test]
    fn difference_with(a in arb_owned_tree(), b in arb_owned_tree()) {
        let r1 = a.left_combine(&b, |a, _| Some(a.to_owned()));
        let mut r2 = a.clone();
        r2.left_combine_with(&b, |_, _| {});
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));

        let r1 = a.left_combine(&b, |_, b| Some(b.to_owned()));
        let mut r2 = a.clone();
        r2.left_combine_with(&b, |a, b| a.set(Some(b)));
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
    }

    #[test]
    fn intersects(a in arb_tree_contents(), b in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        let keys_intersect = at.inner_combine_pred(&bt, |_, _| true);
        let keys_intersect_ref = a.keys().any(|ak| b.contains_key(ak));
        prop_assert_eq!(keys_intersect, keys_intersect_ref);
    }

    #[test]
    fn intersects_sample(a in arb_owned_tree(), b in arb_owned_tree()) {
        let keys_intersect = a.inner_combine_pred(&b, |_, _| true);
        prop_assert!(binary_property_test(&a, &b, !keys_intersect, |a, b| !(a.is_some() & b.is_some())));
    }

    #[test]
    fn is_subset(a in arb_tree_contents(), b in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
        let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
        prop_assert_eq!(at_subset_bt, at_subset_bt_ref);
    }

    #[test]
    fn is_subset_sample(a in arb_owned_tree(), b in arb_owned_tree()) {
        let is_not_subset = a.left_combine_pred(&b, |_, _| false);
        prop_assert!(binary_property_test(&a, &b, !is_not_subset, |a, b| a.is_none() | b.is_some()))
    }

    #[test]
    fn retain_prefix_with(a in arb_tree_contents(), b in arb_tree_contents()) {
        let mut at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        at.retain_prefix_with(&bt, |_| true);
        let mut expected = a;
        expected.retain(|k, _| b.keys().any(|bk| k.starts_with(bk)));
        let actual = to_btree_map(&at);
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_prefix_with(a in arb_tree_contents(), b in arb_tree_contents()) {
        let mut at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        at.remove_prefix_with(&bt, |_| true);
        let mut expected = a;
        expected.retain(|k, _| !b.keys().any(|bk| k.starts_with(bk)));
        let actual = to_btree_map(&at);
        assert_eq!(expected, actual);
    }

    #[test]
    fn group_by_true(a in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        let trees: Vec<RadixTree> = at.group_by(|_, _| true).collect::<Vec<_>>();
        prop_assert_eq!(a.len(), trees.len());
        for ((k0, v0), tree) in a.iter().zip(trees) {
            let k0: &[u8] = k0;
            let v0: &[u8] = v0;
            let k1 = tree.node.load_prefix(&Detached).unwrap().to_vec();
            let v1 = tree.node.value_opt().map(|x| x.to_vec());
            prop_assert!(tree.node.is_leaf());
            prop_assert_eq!(k0, k1);
            prop_assert_eq!(Some(v0.to_vec()), v1);
        }
    }

    #[test]
    fn group_by_fixed(a in arb_tree_contents(), n in 0usize..8) {
        let at = mk_owned_tree(&a);
        let trees: Vec<RadixTree> = at.group_by(|x, _| x.len() <= n).collect::<Vec<_>>();
        prop_assert!(trees.len() <= a.len());
        let mut leafs = BTreeMap::new();
        for tree in &trees {
            let prefix = tree.node.load_prefix(&Detached).unwrap().to_vec();
            if prefix.len() <= n {
                prop_assert!(tree.node.is_leaf());
                prop_assert!(tree.node.value_opt().is_some());
                let value = tree.node.value_opt().unwrap().to_vec();
                let prev = leafs.insert(prefix, value);
                prop_assert!(prev.is_none());
            } else {
                for (k, v) in tree.iter() {
                    let prev = leafs.insert(k.to_vec(), v.to_vec());
                    prop_assert!(prev.is_none());
                }
            }
        }
        prop_assert_eq!(a, leafs);
    }

    #[test]
    fn first_last_value_entry(a in arb_tree_contents()) {
        let at = mk_owned_tree(&a);
        prop_assert_eq!(at.first_value().map(|x| x.to_vec()), a.values().next().map(|v| v.to_vec()));
        prop_assert_eq!(at.first_entry(Vec::new()).map(|(k, v)| (k.to_vec(), v.to_vec())), a.iter().next().map(|(k, v)| (k.to_vec(), v.to_vec())));
        prop_assert_eq!(at.last_value().map(|x| x.to_vec()), a.values().last().map(|v| v.to_vec()));
        prop_assert_eq!(at.last_entry(Vec::new()).map(|(k, v)| (k.to_vec(), v.to_vec())), a.iter().last().map(|(k, v)| (k.to_vec(), v.to_vec())));
    }
}

#[test]
fn union_with1() {
    let a = btreemap! { vec![1] => vec![], vec![2] => vec![] };
    let b = btreemap! { vec![1, 2] => vec![], vec![2] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.outer_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        rbu_reference.insert(k, v);
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn union_with2() {
    let a = btreemap! { vec![] => vec![] };
    let b = btreemap! { vec![] => vec![0] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.outer_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        rbu_reference.insert(k, v);
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn union_with3() {
    let a = btreemap! { vec![] => vec![] };
    let b = btreemap! { vec![] => vec![], vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.outer_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        rbu_reference.insert(k, v);
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn intersection_with1() {
    let a = btreemap! { vec![1,2] => vec![] };
    let b = btreemap! { vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.inner_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = BTreeMap::new();
    for (k, v) in b.clone() {
        if a.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn intersection_with2() {
    let a = btreemap! { vec![1,2] => vec![] };
    let b = btreemap! { vec![1,3] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.inner_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = BTreeMap::new();
    for (k, v) in b.clone() {
        if a.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn intersection_with3() {
    let a = btreemap! { vec![] => vec![], vec![1] => vec![] };
    let b = btreemap! { vec![] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.inner_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = BTreeMap::new();
    for (k, v) in b.clone() {
        if a.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn intersection_with4() {
    let a = btreemap! { vec![1, 2] => vec![] };
    let b = btreemap! { vec![] => vec![], vec![1, 2] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.inner_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = BTreeMap::new();
    for (k, v) in b.clone() {
        if a.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn difference_with1() {
    let a = btreemap! { vec![] => vec![] };
    let b = btreemap! { vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.left_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        if rbu_reference.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn difference_with2() {
    let a = btreemap! { vec![0;130] => vec![] };
    let b = btreemap! { vec![0;129] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.left_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        if rbu_reference.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn difference_with3() {
    let a = btreemap! { vec![1,2] => vec![0] };
    let b = btreemap! { vec![] => vec![], vec![1,2] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.left_combine_with(&bt, |a, b| a.set(Some(b)));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        if rbu_reference.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}

#[test]
fn is_subset1() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! {};
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn is_subset2() {
    let a = btreemap! { vec![1, 1, 1] => vec![] };
    let b = btreemap! { vec![1, 2] => vec![] };
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn is_subset3() {
    let a = btreemap! { vec![1, 1] => vec![], vec![1, 2] => vec![] };
    let b = btreemap! {};
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn is_subset4() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! { vec![] => vec![], vec![1] => vec![] };
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn is_subset5() {
    let a = btreemap! { vec![] => vec![], vec![1] => vec![] };
    let b = btreemap! { vec![] => vec![], vec![2] => vec![] };
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn is_subset6() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! { vec![1] => vec![], vec![2] => vec![] };
    let at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    let at_subset_bt = !at.left_combine_pred(&bt, |_, _| false);
    let at_subset_bt_ref = a.keys().all(|ak| b.contains_key(ak));
    assert_eq!(at_subset_bt, at_subset_bt_ref);
}

#[test]
fn retain_prefix_with1() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! { vec![2] => vec![], vec![3] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.retain_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! {});
}

#[test]
fn retain_prefix_with2() {
    let a = btreemap! { vec![1] => vec![], vec![1, 1] => vec![] };
    let b = btreemap! { vec![1, 1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.retain_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![1, 1] => vec![] });
}

#[test]
fn retain_prefix_with3() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! { vec![] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.retain_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![1] => vec![] });
}

#[test]
fn retain_prefix_with4() {
    let a = btreemap! { vec![1, 2] => vec![] };
    let b = btreemap! { vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.retain_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![1, 2] => vec![] });
}

#[test]
fn remove_prefix_with1() {
    let a = btreemap! { vec![1] => vec![] };
    let b = btreemap! { vec![2] => vec![], vec![3] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.remove_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![1] => vec![] });
}

#[test]
fn remove_prefix_with2() {
    let a = btreemap! { vec![] => vec![] };
    let b = btreemap! {};
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.remove_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![] => vec![] });
}

#[test]
fn remove_prefix_with3() {
    let a = btreemap! { vec![1] => vec![], vec![1, 1] => vec![] };
    let b = btreemap! { vec![1, 1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.remove_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! { vec![1] => vec![] });
}

#[test]
fn remove_prefix_with4() {
    let a = btreemap! { vec![1, 2] => vec![] };
    let b = btreemap! { vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.remove_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! {});
}

#[test]
fn remove_prefix_with5() {
    let a = btreemap! { vec![1, 2] => vec![] };
    let b = btreemap! { vec![1, 2] => vec![], vec![1, 3] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.remove_prefix_with(&bt, |_| true);
    let r = to_btree_map(&at);
    assert_eq!(r, btreemap! {});
}
