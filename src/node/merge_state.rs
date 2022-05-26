#![allow(dead_code)]
use crate::{
    iterators::SliceIterator,
    node::{TreeNode, TreeValue},
    store::{BlobStore, NoError, NoStore},
};
use binary_merge::{MergeOperation, MergeState};
use core::{fmt, fmt::Debug};
use inplace_vec_builder::InPlaceVecBuilder;
use std::marker::PhantomData;

/// A typical write part for the merge state
pub(crate) trait MergeStateMut: MergeState {
    /// Consume n elements of a
    fn advance_a(&mut self, n: usize, take: bool) -> bool;
    /// Consume n elements of b
    fn advance_b(&mut self, n: usize, take: bool) -> bool;
}

pub(crate) trait MutateInput: MergeStateMut {
    fn source_slices_mut(&mut self) -> (&mut [Self::A], &[Self::B]);
}

/// An in place merge state where the rhs is a reference
pub(crate) struct InPlaceVecMergeStateRef<'a, T: TT> {
    pub a: InPlaceVecBuilder<'a, TreeNode<T::AB>>,
    pub ab: &'a T::AB,
    pub b: SliceIterator<'a, TreeNode<T::BB>>,
    pub bb: &'a T::BB,
    pub c: T::NC,
    pub err: Option<T::E>,
}

impl<'a, T: TT> InPlaceVecMergeStateRef<'a, T> {
    pub fn new(
        a: &'a mut Vec<TreeNode<T::AB>>,
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
        c: T::NC,
    ) -> Self {
        Self {
            a: a.into(),
            ab,
            b: SliceIterator(b.as_ref()),
            bb,
            c,
            err: None,
        }
    }
}

impl<'a, T: TT> MergeState for InPlaceVecMergeStateRef<'a, T> {
    type A = TreeNode<T::AB>;
    type B = TreeNode<T::BB>;
    fn a_slice(&self) -> &[TreeNode<T::AB>] {
        self.a.source_slice()
    }
    fn b_slice(&self) -> &[TreeNode<T::BB>] {
        self.b.as_slice()
    }
}

impl<'a, T: TT> MergeStateMut for InPlaceVecMergeStateRef<'a, T> {
    fn advance_a(&mut self, n: usize, take: bool) -> bool {
        self.a.consume(n, take);
        true
    }
    fn advance_b(&mut self, n: usize, take: bool) -> bool {
        if take {
            let iter = &mut self.b;
            // self.a.extend_from_iter((&mut self.b).cloned(), n);
            for _ in 0..n {
                if let Some(node) = iter.next() {
                    match self.c.convert_node(node, self.bb) {
                        Ok(node) => self.a.push(node),
                        Err(err) => {
                            self.err = Some(err.into());
                            return false;
                        }
                    }
                } else {
                    break;
                }
            }
        } else {
            for _ in 0..n {
                let _ = self.b.next();
            }
        }
        true
    }
}

impl<'a, T: TT> MutateInput for InPlaceVecMergeStateRef<'a, T> {
    fn source_slices_mut(&mut self) -> (&mut [Self::A], &[Self::B]) {
        (self.a.source_slice_mut(), self.b.as_slice())
    }
}

impl<'a, T: TT> InPlaceVecMergeStateRef<'a, T> {
    pub fn merge<O: MergeOperation<Self>>(
        a: &'a mut Vec<TreeNode<T::AB>>,
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
        c: T::NC,
        o: &O,
    ) -> Result<(), T::E> {
        let mut state = Self::new(a, ab, b, bb, c);
        o.merge(&mut state);
        if let Some(err) = state.err {
            Err(err)
        } else {
            Ok(())
        }
    }
}

/// A merge state where we only track if elements have been produced, and abort as soon as the first element is produced
pub(crate) struct BoolOpMergeState<'a, T: TT> {
    pub a: SliceIterator<'a, TreeNode<T::AB>>,
    pub ab: &'a T::AB,
    pub b: SliceIterator<'a, TreeNode<T::BB>>,
    pub bb: &'a T::BB,
    pub r: bool,
    pub err: Option<T::E>,
}

impl<'a, T: TT> Debug for BoolOpMergeState<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a: {:?}, b: {:?} r: {}",
            self.a_slice(),
            self.b_slice(),
            self.r
        )
    }
}

impl<'a, T: TT> BoolOpMergeState<'a, T> {
    fn new(
        a: &'a [TreeNode<T::AB>],
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
    ) -> Self {
        Self {
            a: SliceIterator(a),
            ab,
            b: SliceIterator(b),
            bb,
            err: None,
            r: false,
        }
    }

    pub fn merge<O: MergeOperation<Self>>(
        a: &'a [TreeNode<T::AB>],
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
        o: &O,
    ) -> Result<bool, T::E> {
        let mut state = Self::new(a, ab, b, bb);
        o.merge(&mut state);
        if let Some(err) = state.err {
            Err(err)
        } else {
            Ok(state.r)
        }
    }
}

impl<'a, T: TT> MergeState for BoolOpMergeState<'a, T> {
    type A = TreeNode<T::AB>;
    type B = TreeNode<T::BB>;
    fn a_slice(&self) -> &[TreeNode<T::AB>] {
        self.a.as_slice()
    }
    fn b_slice(&self) -> &[TreeNode<T::BB>] {
        self.b.as_slice()
    }
}

impl<'a, T: TT> MergeStateMut for BoolOpMergeState<'a, T> {
    fn advance_a(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.r = true;
            false
        } else {
            self.a.drop_front(n);
            true
        }
    }
    fn advance_b(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.r = true;
            false
        } else {
            self.b.drop_front(n);
            true
        }
    }
}

pub trait TT: Default {
    type AB: BlobStore;
    type BB: BlobStore;
    type E: From<<<Self as TT>::AB as BlobStore>::Error>
        + From<<<Self as TT>::BB as BlobStore>::Error>;
    type NC: NodeConverter<<Self as TT>::BB, <Self as TT>::AB>;
}

pub(crate) struct TTI<AB, BB, E, NC = NoConverter>(PhantomData<(AB, BB, E, NC)>);

impl<AB, BB, E, NC> Default for TTI<AB, BB, E, NC> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<
        AB: BlobStore,
        BB: BlobStore,
        E: From<AB::Error> + From<BB::Error>,
        NC: NodeConverter<BB, AB>,
    > TT for TTI<AB, BB, E, NC>
{
    type AB = AB;

    type BB = BB;

    type E = E;

    type NC = NC;
}

impl<AB, BB, E, NC> TTI<AB, BB, E, NC> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub struct NoStoreT;

impl TT for NoStoreT {
    type AB = NoStore;

    type BB = NoStore;

    type E = NoError;

    type NC = NoStoreConverter;
}

/// A node converter describes how to completely convert nodes attached to one store `A` to nodes attached to another store `B`.
///
/// There is a zero cost converter `NoStoreConverter` for converting from NoStore to NoStore, and an universal converter
/// `DetachConverter` that just detaches from `A` and then downcasts to `B` without attaching.
///
/// But you could also write a more clever converter that e.g. maps ids.
pub trait NodeConverter<A: BlobStore, B: BlobStore>: Copy {
    /// Convert a value from being attached to store `A` to a node belonging to store `B` (not necessarily attached)
    fn convert_value(&self, value: &TreeValue<A>, store: &A) -> Result<TreeValue<B>, A::Error> {
        Ok(value.detached(store)?.downcast())
    }
    /// Convert a node from being attached to store `A` to a node belonging to store `B` (not necessarily attached)
    fn convert_node(&self, node: &TreeNode<A>, store: &A) -> Result<TreeNode<B>, A::Error> {
        Ok(node.detached(store)?.downcast())
    }
    /// Convert a node from being attached to store `A` to a node belonging to store `B` (not necessarily attached)...
    ///
    /// and drop n bytes from the prefix
    fn convert_node_shortened(
        &self,
        node: &TreeNode<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<B>, A::Error>;
}

/// A converter that can just convert from NoStore to NoStore, which is just the identity conversion
#[derive(Clone, Copy)]
pub struct NoStoreConverter;

impl NodeConverter<NoStore, NoStore> for NoStoreConverter {
    fn convert_node(&self, node: &TreeNode, _: &NoStore) -> Result<TreeNode, NoError> {
        Ok(node.clone())
    }
    fn convert_value(&self, value: &TreeValue, _: &NoStore) -> Result<TreeValue, NoError> {
        Ok(value.clone())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNode,
        store: &NoStore,
        n: usize,
    ) -> Result<TreeNode, NoError> {
        node.clone_shortened(store, n)
    }
}

/// A converter that converts from one store to another store by just completely detaching it
#[derive(Clone, Copy)]
pub struct DetachConverter;

impl<A: BlobStore, B: BlobStore> NodeConverter<A, B> for DetachConverter {
    fn convert_node(&self, node: &TreeNode<A>, store: &A) -> Result<TreeNode<B>, A::Error> {
        Ok(node.detached(store)?.downcast())
    }
    fn convert_value(&self, value: &TreeValue<A>, store: &A) -> Result<TreeValue<B>, A::Error> {
        Ok(value.detached(store)?.downcast())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNode<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<B>, A::Error> {
        let node = node.clone_shortened(store, n)?;
        Ok(self.convert_node(&node, store)?.downcast())
    }
}

/// A converter that converts from NoStore to any store by just downcasting without actually attaching anything
#[derive(Clone, Copy)]
pub struct DowncastConverter;

impl<A: BlobStore> NodeConverter<NoStore, A> for DowncastConverter {
    fn convert_node(&self, node: &TreeNode, _: &NoStore) -> Result<TreeNode<A>, NoError> {
        Ok(node.downcast())
    }
    fn convert_value(&self, value: &TreeValue, _: &NoStore) -> Result<TreeValue<A>, NoError> {
        Ok(value.downcast())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNode,
        store: &NoStore,
        n: usize,
    ) -> Result<TreeNode<A>, NoError> {
        let node = node.clone_shortened(store, n)?;
        Ok(node.downcast())
    }
}

/// A converter that will panic on each call, used only internally for cases where a converter is not needed
#[derive(Clone, Copy)]
pub(crate) struct NoConverter;

impl<A: BlobStore, B: BlobStore> NodeConverter<A, B> for NoConverter {
    fn convert_node(&self, _: &TreeNode<A>, _: &A) -> Result<TreeNode<B>, <A as BlobStore>::Error> {
        panic!()
    }

    fn convert_value(
        &self,
        _: &crate::node::TreeValue<A>,
        _: &A,
    ) -> Result<crate::node::TreeValue<B>, <A as BlobStore>::Error> {
        panic!()
    }

    fn convert_node_shortened(
        &self,
        _: &TreeNode<A>,
        _: &A,
        _: usize,
    ) -> Result<TreeNode<B>, A::Error> {
        panic!()
    }
}

/// A merge state where we build into a new vec
pub(crate) struct VecMergeState<'a, T: TT> {
    pub a: SliceIterator<'a, TreeNode<T::AB>>,
    pub ab: &'a T::AB,
    pub b: SliceIterator<'a, TreeNode<T::BB>>,
    pub bb: &'a T::BB,
    pub r: Vec<TreeNode>,
    pub err: Option<T::E>,
}

impl<'a, T: TT> Debug for VecMergeState<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a: {:?}, b: {:?}", self.a_slice(), self.b_slice(),)
    }
}

impl<'a, T: TT> VecMergeState<'a, T> {
    fn new(
        a: &'a [TreeNode<T::AB>],
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
        r: Vec<TreeNode>,
    ) -> Self {
        Self {
            a: SliceIterator(a),
            ab,
            b: SliceIterator(b),
            bb,
            r,
            err: None,
        }
    }

    fn into_vec(self) -> std::result::Result<Vec<TreeNode>, T::E> {
        if let Some(err) = self.err {
            Err(err)
        } else {
            Ok(self.r)
        }
    }

    pub fn merge<O: MergeOperation<Self>>(
        a: &'a [TreeNode<T::AB>],
        ab: &'a T::AB,
        b: &'a [TreeNode<T::BB>],
        bb: &'a T::BB,
        o: &'a O,
    ) -> std::result::Result<Vec<TreeNode>, T::E> {
        let t: Vec<TreeNode> = Vec::new();
        let mut state = Self::new(a, ab, b, bb, t);
        o.merge(&mut state);
        state.into_vec()
    }
}

impl<'a, T: TT> MergeState for VecMergeState<'a, T> {
    type A = TreeNode<T::AB>;
    type B = TreeNode<T::BB>;
    fn a_slice(&self) -> &[TreeNode<T::AB>] {
        self.a.as_slice()
    }
    fn b_slice(&self) -> &[TreeNode<T::BB>] {
        self.b.as_slice()
    }
}

impl<'a, T: TT> MergeStateMut for VecMergeState<'a, T> {
    fn advance_a(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.r.reserve(n);
            for e in self.a.take_front(n).iter() {
                match e.detached(self.ab) {
                    Ok(e) => self.r.push(e),
                    Err(cause) => {
                        self.err = Some(cause.into());
                        return false;
                    }
                }
            }
        } else {
            self.a.drop_front(n);
        }
        true
    }
    fn advance_b(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.r.reserve(n);
            for e in self.b.take_front(n).iter() {
                match e.detached(self.bb) {
                    Ok(e) => self.r.push(e),
                    Err(cause) => {
                        self.err = Some(cause.into());
                        return false;
                    }
                }
            }
        } else {
            self.b.drop_front(n);
        }
        true
    }
}