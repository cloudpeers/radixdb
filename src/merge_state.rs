#![allow(dead_code)]
use crate::{
    blob_store::NoStore, iterators::SliceIterator, tree::TreeNode, BlobStore, DynBlobStore,
};
use binary_merge::{MergeOperation, MergeState};
use core::{fmt, fmt::Debug};
use inplace_vec_builder::InPlaceVecBuilder;
use std::{convert::Infallible, marker::PhantomData};

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
pub(crate) struct InPlaceVecMergeStateRef<'a, R> {
    pub(crate) a: InPlaceVecBuilder<'a, R>,
    pub(crate) b: SliceIterator<'a, R>,
}

impl<'a, R> InPlaceVecMergeStateRef<'a, R> {
    pub fn new(a: &'a mut Vec<R>, b: &'a impl AsRef<[R]>) -> Self {
        Self {
            a: a.into(),
            b: SliceIterator(b.as_ref()),
        }
    }
}

impl<'a, R> MergeState for InPlaceVecMergeStateRef<'a, R> {
    type A = R;
    type B = R;
    fn a_slice(&self) -> &[R] {
        self.a.source_slice()
    }
    fn b_slice(&self) -> &[R] {
        self.b.as_slice()
    }
}

impl<'a, R> MergeStateMut for InPlaceVecMergeStateRef<'a, R>
where
    R: Clone,
{
    fn advance_a(&mut self, n: usize, take: bool) -> bool {
        self.a.consume(n, take);
        true
    }
    fn advance_b(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.a.extend_from_iter((&mut self.b).cloned(), n);
        } else {
            for _ in 0..n {
                let _ = self.b.next();
            }
        }
        true
    }
}

impl<'a, R> MutateInput for InPlaceVecMergeStateRef<'a, R>
where
    R: Clone,
{
    fn source_slices_mut(&mut self) -> (&mut [Self::A], &[Self::B]) {
        (self.a.source_slice_mut(), self.b.as_slice())
    }
}

impl<'a, R> InPlaceVecMergeStateRef<'a, R> {
    pub fn merge<O: MergeOperation<Self>>(a: &'a mut Vec<R>, b: &'a impl AsRef<[R]>, o: O) {
        let mut state = Self::new(a, b);
        o.merge(&mut state);
    }
}

/// A merge state where we only track if elements have been produced, and abort as soon as the first element is produced
pub(crate) struct BoolOpMergeState<'a, A, B> {
    a: SliceIterator<'a, A>,
    b: SliceIterator<'a, B>,
    r: bool,
}

impl<'a, A: Debug, B: Debug> Debug for BoolOpMergeState<'a, A, B> {
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

impl<'a, A, B> BoolOpMergeState<'a, A, B> {
    fn new(a: &'a [A], b: &'a [B]) -> Self {
        Self {
            a: SliceIterator(a),
            b: SliceIterator(b),
            r: false,
        }
    }
}

impl<'a, A, B> BoolOpMergeState<'a, A, B> {
    pub fn merge<O: MergeOperation<Self>>(a: &'a [A], b: &'a [B], o: O) -> bool {
        let mut state = Self::new(a, b);
        o.merge(&mut state);
        state.r
    }
}

impl<'a, A, B> MergeState for BoolOpMergeState<'a, A, B> {
    type A = A;
    type B = B;
    fn a_slice(&self) -> &[A] {
        self.a.as_slice()
    }
    fn b_slice(&self) -> &[B] {
        self.b.as_slice()
    }
}

impl<'a, A, B> MergeStateMut for BoolOpMergeState<'a, A, B> {
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
}

pub struct TTI<AB, BB, E>(PhantomData<(AB, BB, E)>);

impl<AB, BB, E> Default for TTI<AB, BB, E> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<AB: BlobStore, BB: BlobStore, E: From<AB::Error> + From<BB::Error>> TT for TTI<AB, BB, E> {
    type AB = AB;

    type BB = BB;

    type E = E;
}

impl<AB, BB, E> TTI<AB, BB, E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub struct DynT;

impl TT for DynT {
    type AB = DynBlobStore;

    type BB = DynBlobStore;

    type E = anyhow::Error;
}

#[derive(Default)]
pub struct NoStoreT;

impl TT for NoStoreT {
    type AB = NoStore;

    type BB = NoStore;

    type E = Infallible;
}

/// A merge state where we build into a new vec
pub(crate) struct VecMergeState<'a, T: TT> {
    pub a: SliceIterator<'a, TreeNode>,
    pub ab: &'a T::AB,
    pub b: SliceIterator<'a, TreeNode>,
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
        a: &'a [TreeNode],
        ab: &'a T::AB,
        b: &'a [TreeNode],
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
        a: &'a [TreeNode],
        ab: &'a T::AB,
        b: &'a [TreeNode],
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
    type A = TreeNode;
    type B = TreeNode;
    fn a_slice(&self) -> &[TreeNode] {
        self.a.as_slice()
    }
    fn b_slice(&self) -> &[TreeNode] {
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
