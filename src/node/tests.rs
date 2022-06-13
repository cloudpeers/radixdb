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

impl TestSamples<Vec<u8>, Option<Vec<u8>>> for Tree {
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
        r2.outer_combine_with(&b, |a, b| a.set(b));
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
        prop_assert!(binary_element_test(&a, &b, r, |a, b| b.and_then(|_| a)));

        let r = a.inner_combine(&b, |_, b| Some(b.to_owned()));
        prop_assert!(binary_element_test(&a, &b, r, |a, b| a.and_then(|_| b)));
    }

    #[test]
    fn intersection_with(a in arb_owned_tree(), b in arb_owned_tree()) {
        // right biased intersection
        let r1 = a.inner_combine(&b, |_, b| Some(b.to_owned()));
        let mut r2 = a.clone();
        r2.inner_combine_with(&b, |a, b| a.set(b));
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
        // // left biased intersection
        // let r1 = a.inner_combine(&b, |a, _| Some(a.to_owned()));
        // let mut r2 = a.clone();
        // r2.inner_combine_with(&b, |a, _| {});
        // prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
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
        r2.left_combine_with(&b, |a, b| {});
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));

        let r1 = a.left_combine(&b, |_, b| Some(b.to_owned()));
        let mut r2 = a.clone();
        r2.left_combine_with(&b, |a, b| a.set(b));
        prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
    }
}

#[test]
fn intersection_with1() {
    let a = btreemap! { vec![1,2] => vec![] };
    let b = btreemap! { vec![1] => vec![] };
    let mut at = mk_owned_tree(&a);
    let bt = mk_owned_tree(&b);
    at.inner_combine_with(&bt, |a, b| a.set(b));
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
    at.inner_combine_with(&bt, |a, b| a.set(b));
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
    at.inner_combine_with(&bt, |a, b| a.set(b));
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
    at.inner_combine_with(&bt, |a, b| a.set(b));
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
    at.left_combine_with(&bt, |a, b| a.set(b));
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
    at.left_combine_with(&bt, |a, b| a.set(b));
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
    at.left_combine_with(&bt, |a, b| a.set(b));
    let rbu = to_btree_map(&at);
    let mut rbu_reference = a.clone();
    for (k, v) in b.clone() {
        if rbu_reference.contains_key(&k) {
            rbu_reference.insert(k, v);
        }
    }
    assert_eq!(rbu, rbu_reference);
}
