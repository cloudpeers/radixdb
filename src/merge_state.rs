#![allow(dead_code)]
use crate::{iterators::SliceIterator, tree::TreeNode};
use binary_merge::{MergeOperation, MergeState};
use core::{fmt, fmt::Debug};
use inplace_vec_builder::InPlaceVecBuilder;

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

/// A merge state where we build into a new vec
pub(crate) struct VecMergeState<'a, T> {
    pub a: SliceIterator<'a, TreeNode<T>>,
    pub b: SliceIterator<'a, TreeNode<T>>,
    pub r: Vec<TreeNode<T>>,
    pub err: Option<anyhow::Error>,
}

impl<'a, T> Debug for VecMergeState<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a: {:?}, b: {:?}", self.a_slice(), self.b_slice(),)
    }
}

impl<'a, T> VecMergeState<'a, T> {
    fn new(a: &'a [TreeNode<T>], b: &'a [TreeNode<T>], r: Vec<TreeNode<T>>) -> Self {
        Self {
            a: SliceIterator(a),
            b: SliceIterator(b),
            r,
            err: None,
        }
    }

    fn into_vec(self) -> anyhow::Result<Vec<TreeNode<T>>> {
        if let Some(err) = self.err {
            Err(err)
        } else {
            Ok(self.r)
        }
    }

    pub fn merge<O: MergeOperation<Self>>(
        a: &'a [TreeNode<T>],
        b: &'a [TreeNode<T>],
        o: &'a O,
    ) -> anyhow::Result<Vec<TreeNode<T>>> {
        let t: Vec<TreeNode<T>> = Vec::new();
        let mut state = Self::new(a, b, t);
        o.merge(&mut state);
        state.into_vec()
    }
}

impl<'a, T> MergeState for VecMergeState<'a, T> {
    type A = TreeNode<T>;
    type B = TreeNode<T>;
    fn a_slice(&self) -> &[TreeNode<T>] {
        self.a.as_slice()
    }
    fn b_slice(&self) -> &[TreeNode<T>] {
        self.b.as_slice()
    }
}

impl<'a, T> MergeStateMut for VecMergeState<'a, T> {
    fn advance_a(&mut self, n: usize, take: bool) -> bool {
        if take {
            self.r.reserve(n);
            for e in self.a.take_front(n).iter() {
                self.r.push(e.clone())
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
                self.r.push(e.clone())
            }
        } else {
            self.b.drop_front(n);
        }
        true
    }
}
