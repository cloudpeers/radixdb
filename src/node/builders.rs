use std::{fmt, fmt::Debug, marker::PhantomData, sync::Arc};

use smallvec::SmallVec;

use crate::{
    node::iterators::NodeSeqIter,
    store::{Blob2 as Blob, BlobStore2 as BlobStore, NoStore, OwnedBlob},
    Hex,
};

use super::{
    iterators::FlexRefIter,
    refs::{
        make_header_byte, FlexRef, TreeChildrenRef, TreeNode, TreePrefixRef, TreeValueOptRef, Type,
        NONE, PTR8,
    },
};

#[derive(Debug)]
pub(crate) struct AtPrefix;
#[derive(Debug)]
pub(crate) struct AtValue;
#[derive(Debug)]
pub(crate) struct AtChildren;

pub(crate) trait IterPosition: Debug {}
impl IterPosition for AtPrefix {}
impl IterPosition for AtValue {}
impl IterPosition for AtChildren {}

pub(crate) trait VecTakeExt {
    fn take(&mut self) -> Self;
}

impl VecTakeExt for SmallVec<[u8; 16]> {
    fn take(&mut self) -> Self {
        let mut t = SmallVec::new();
        std::mem::swap(&mut t, self);
        t
    }
}

#[derive(Default)]

pub(crate) struct InPlaceFlexRefSeqBuilder {
    // place for the data. [..t1] and [s1..] contain valid sequences of flexrefs.
    // The rest is to be considered uninitialized
    vec: SmallVec<[u8; 16]>,
    // end of the result
    t1: usize,
    // start of the source
    s0: usize,
}

impl Debug for InPlaceFlexRefSeqBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}, {}]",
            Hex::new(self.target_slice()),
            Hex::new(self.source_slice())
        )
    }
}

pub(crate) trait Extendable {
    fn reserve(&mut self, n: usize);

    fn extend_from_slice(&mut self, data: &[u8]);

    fn push(&mut self, value: u8);

    fn push_id(&mut self, id: &[u8]) {
        self.push(make_header_byte(Type::Id, id.len()));
        self.extend_from_slice(id);
    }

    fn push_none(&mut self) {
        self.push(NONE);
    }

    fn push_arc<T>(&mut self, arc: Arc<T>) {
        let data: usize = Arc::into_raw(arc) as usize;
        let data: u64 = data as u64;
        self.push(make_header_byte(Type::Ptr, 8));
        self.extend_from_slice(&data.to_be_bytes());
    }

    fn push_arc_or_inline(&mut self, data: &[u8]) {
        let data = data.as_ref();
        if data.len() < 64 {
            self.push_inline(data.as_ref())
        } else {
            self.push_arc(Arc::new(data.to_vec()))
        }
    }

    fn push_inline(&mut self, data: &[u8]) {
        self.push(make_header_byte(Type::Inline, data.len()));
        self.extend_from_slice(data);
    }
}

impl Extendable for SmallVec<[u8; 16]> {
    fn reserve(&mut self, n: usize) {
        let free = self.capacity() - self.len();
        if free < n {
            self.reserve(n - free);
        }
    }

    fn push(&mut self, value: u8) {
        self.push(value)
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data)
    }
}

impl Extendable for InPlaceFlexRefSeqBuilder {
    fn reserve(&mut self, n: usize) {
        let gap = self.gap();
        if gap < n {
            let missing = n - gap;
            self.vec.reserve(missing);
            let s1 = self.vec.len();
            let space = self.vec.capacity() - s1;
            self.vec.resize(self.vec.capacity(), 0);
            self.vec.copy_within(self.s0..s1, self.s0 + space);
            self.s0 += space;
        }
    }

    fn push(&mut self, value: u8) {
        self.reserve(1);
        self.vec[self.t1] = value;
        self.t1 += 1;
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        let len = data.len();
        self.reserve(len);
        self.vec[self.t1..self.t1 + len].copy_from_slice(data);
        self.t1 += len;
    }
}

fn validate_flexref_slice(value: &[u8]) -> usize {
    let mut iter = FlexRefIter::new(value);
    let mut n = 0;
    while let Some(item) = iter.next() {
        // debug_assert!(!item.is_empty());
        n += 1;
    }
    // debug_assert!(iter.is_empty());
    n
}

fn validate_nodeseq_slice<S: BlobStore>(value: &[u8]) -> usize {
    let mut iter = NodeSeqIter::<S>::new(value);
    let mut n = 0;
    while let Some(item) = iter.next() {
        debug_assert!(item.prefix().is_valid());
        debug_assert!(item.value().is_valid());
        debug_assert!(item.children().is_valid());
        n += 1;
    }
    debug_assert!(iter.is_empty());
    n
}

impl InPlaceFlexRefSeqBuilder {
    /// Create a new builder by taking over the data from a vec
    pub fn new(vec: SmallVec<[u8; 16]>) -> Self {
        Self { vec, t1: 0, s0: 0 }
    }

    fn into_inner(mut self) -> SmallVec<[u8; 16]> {
        // debug_assert!(self.source_count() == 0);
        // debug_assert!(self.target_count() % 3 == 0);
        self.vec.truncate(self.t1);
        self.vec
    }

    fn take_result(&mut self) -> SmallVec<[u8; 16]> {
        // debug_assert!(self.source_count() == 0);
        // debug_assert!(self.target_count() % 3 == 0);
        self.vec.truncate(self.t1);
        let mut res = SmallVec::new();
        std::mem::swap(&mut self.vec, &mut res);
        self.t1 = 0;
        self.s0 = 0;
        res
    }

    pub fn source_count(&self) -> usize {
        validate_flexref_slice(self.source_slice())
    }

    pub fn target_count(&self) -> usize {
        validate_flexref_slice(self.source_slice())
    }

    pub fn total_count(&self) -> usize {
        self.source_count() + self.target_count()
    }

    fn drop(&mut self, n: usize) {
        // debug_assert!(n <= self.source_slice().len());
        self.s0 += n;
    }

    fn forward_all(&mut self) {
        self.forward(self.source_slice().len())
    }

    fn rewind_all(&mut self) {
        self.rewind(self.target_slice().len())
    }

    fn forward(&mut self, n: usize) {
        // debug_assert!(n <= self.source_slice().len());
        if self.gap() == 0 {
            self.t1 += n;
            self.s0 += n;
        } else {
            self.vec.copy_within(self.s0..self.s0 + n, self.t1);
            self.t1 += n;
            self.s0 += n;
        }
    }

    fn rewind(&mut self, to: usize) {
        // debug_assert!(to <= self.t1);
        let n = self.t1 - to;
        if self.gap() == 0 {
            self.t1 -= n;
            self.s0 -= n;
        } else {
            self.vec.copy_within(to..to + n, self.s0 - n);
            self.t1 -= n;
            self.s0 -= n;
        }
    }

    #[inline]
    const fn gap(&self) -> usize {
        self.s0 - self.t1
    }

    #[inline]
    fn target_slice(&self) -> &[u8] {
        &self.vec[..self.t1]
    }

    #[inline]
    fn source_slice(&self) -> &[u8] {
        &self.vec[self.s0..]
    }

    #[inline]
    fn has_remaining(&self) -> bool {
        self.s0 < self.vec.len()
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct BuilderRef<'a, S: BlobStore, P: IterPosition>(
    &'a mut NodeSeqBuilder<S>,
    PhantomData<P>,
);

impl<'a, S: BlobStore, P: IterPosition> BuilderRef<'a, S, P> {
    fn done<P2: IterPosition>(self) -> BuilderRef<'a, S, P2> {
        BuilderRef(self.0, PhantomData)
    }
}

impl<'a, S: BlobStore> BuilderRef<'a, S, AtPrefix> {
    pub fn push_prefix(self, prefix: impl AsRef<[u8]>) -> BuilderRef<'a, S, AtValue> {
        self.0.data.push_arc_or_inline(prefix.as_ref());
        self.done()
    }

    pub fn push_prefix_raw(self, value: &TreePrefixRef<S>) -> BuilderRef<'a, S, AtValue> {
        value.manual_clone();
        self.0.data.extend_from_slice(value.bytes());
        self.done()
    }

    pub fn push_prefix_empty(mut self) -> BuilderRef<'a, S, AtValue> {
        self.0.data.push_inline(&[]);
        self.done()
    }
}

impl<'a, S: BlobStore> BuilderRef<'a, S, AtValue> {
    pub fn push_value(self, value: impl AsRef<[u8]>) -> BuilderRef<'a, S, AtChildren> {
        self.0.data.push_arc_or_inline(value.as_ref());
        self.done()
    }

    pub fn push_value_opt(self, value: Option<impl AsRef<[u8]>>) -> BuilderRef<'a, S, AtChildren> {
        if let Some(value) = value {
            self.0.data.push_arc_or_inline(value.as_ref());
        } else {
            self.0.data.push_none();
        }
        self.done()
    }

    pub fn push_value_none(mut self) -> BuilderRef<'a, S, AtChildren> {
        self.0.data.push_none();
        self.done()
    }

    pub fn push_value_raw(self, value: &TreeValueOptRef<S>) -> BuilderRef<'a, S, AtChildren> {
        value.manual_clone();
        self.0.data.extend_from_slice(value.bytes());
        self.done()
    }
}

impl<'a, S: BlobStore> BuilderRef<'a, S, AtChildren> {
    pub fn push_children(self, children: NodeSeqBuilder<S>) -> BuilderRef<'a, S, AtPrefix> {
        if !children.is_empty() {
            self.0.data.push_arc(Arc::new(children));
        } else {
            self.0.data.push_none();
        }
        self.done()
    }

    pub fn push_children_raw(self, value: &TreeChildrenRef<S>) -> BuilderRef<'a, S, AtPrefix> {
        value.manual_clone();
        self.0.data.extend_from_slice(value.bytes());
        self.done()
    }

    pub fn push_children_empty(self) -> BuilderRef<'a, S, AtChildren> {
        self.0.data.push_none();
        self.done()
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct InPlaceBuilderRef<'a, S: BlobStore, P: IterPosition>(
    &'a mut InPlaceFlexRefSeqBuilder,
    PhantomData<(S, P)>,
);

impl<'a, S: BlobStore, P: IterPosition> InPlaceBuilderRef<'a, S, P> {
    fn done<P2: IterPosition>(self) -> InPlaceBuilderRef<'a, S, P2> {
        InPlaceBuilderRef(self.0, PhantomData)
    }
}

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtPrefix> {
    pub fn peek(&self) -> &TreePrefixRef<S> {
        TreePrefixRef::read(self.0.source_slice()).unwrap()
    }

    pub fn move_prefix(self) -> InPlaceBuilderRef<'a, S, AtValue> {
        let len = self.peek().bytes().len();
        self.0.forward(len);
        self.done()
    }

    pub fn move_prefix_shortened(
        self,
        n: usize,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        // todo: make this more efficient for the extremely common "inline" case
        let prefix = self.peek().load(store)?.to_owned();
        Ok(self.push_prefix(&prefix[..n]))
    }

    pub fn attach_prefix(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        match self.peek().arc_to_id(store)? {
            Ok(id) => {
                self.drop_current();
                self.0.push_id(&id);
            }
            Err(n) => self.0.forward(n),
        }
        Ok(self.done())
    }

    pub fn detach_prefix(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        match self.peek().id_to_arc(store)? {
            Ok(id) => {
                self.drop_current();
                self.0.push_arc_or_inline(&id);
            }
            Err(n) => self.0.forward(n),
        }
        Ok(self.done())
    }

    pub fn push_prefix_empty(mut self) -> InPlaceBuilderRef<'a, S, AtValue> {
        self.drop_current();
        self.0.push_inline(&[]);
        self.done()
    }

    pub fn push_prefix(mut self, prefix: impl AsRef<[u8]>) -> InPlaceBuilderRef<'a, S, AtValue> {
        self.drop_current();
        self.0.push_arc_or_inline(prefix.as_ref());
        self.done()
    }

    pub fn insert_converted<S2: BlobStore>(
        mut self,
        prefix: &TreePrefixRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        prefix.manual_clone();
        self.0.extend_from_slice(prefix.bytes());
        Ok(self.done())
    }

    fn drop_current(&mut self) {
        let peek = self.peek();
        let len = peek.bytes().len();
        peek.manual_drop();
        self.0.drop(len);
    }

    pub fn mark(&self) -> usize {
        self.0.t1
    }

    pub fn rewind(self, to: usize) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.0.rewind(to);
        self.done()
    }
}

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtValue> {
    fn drop_current(&mut self) {
        let len = self.peek().bytes().len();
        self.peek().manual_drop();
        self.0.drop(len);
    }

    pub fn peek(&self) -> &TreeValueOptRef<S> {
        TreeValueOptRef::read(self.0.source_slice()).unwrap()
    }

    pub fn move_value(self) -> InPlaceBuilderRef<'a, S, AtChildren> {
        let len = self.peek().bytes().len();
        self.0.forward(len);
        self.done()
    }

    pub fn push_value_none(mut self) -> InPlaceBuilderRef<'a, S, AtChildren> {
        self.drop_current();
        self.0.push_none();
        self.done()
    }

    pub fn push_value_opt(
        self,
        value: Option<impl AsRef<[u8]>>,
    ) -> InPlaceBuilderRef<'a, S, AtChildren> {
        if let Some(value) = value {
            self.push_value(value.as_ref())
        } else {
            self.push_value_none()
        }
    }

    pub fn push_value(mut self, value: &[u8]) -> InPlaceBuilderRef<'a, S, AtChildren> {
        self.drop_current();
        self.0.push_arc_or_inline(value);
        self.done()
    }

    pub fn attach_value(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        match self.peek().arc_to_id(store)? {
            Ok(id) => {
                self.drop_current();
                self.0.push_id(&id);
            }
            Err(n) => self.0.forward(n),
        }
        Ok(self.done())
    }

    pub fn detach_value(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        match self.peek().id_to_arc(store)? {
            Ok(id) => {
                self.drop_current();
                self.0.push_arc_or_inline(&id);
            }
            Err(n) => self.0.forward(n),
        }
        Ok(self.done())
    }

    pub fn push_converted<S2: BlobStore>(
        mut self,
        value: &TreeValueOptRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        self.insert_converted(value, store)
    }

    pub fn insert_converted<S2: BlobStore>(
        self,
        value: &TreeValueOptRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        value.manual_clone();
        self.0.extend_from_slice(value.bytes());
        Ok(self.done())
    }
}

// use lazy_static::lazy_static;

// lazy_static! {
//    static ref MUTATE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::default();
//    static ref GROW_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::default();
// }

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtChildren> {
    /// peek the child at the current position as a TreeChildrenRef
    ///
    /// this returns an error in case the item extends behind the end of the buffer
    pub fn peek(&self) -> &TreeChildrenRef<S> {
        TreeChildrenRef::read(self.0.source_slice()).unwrap()
    }

    pub fn mutate(
        mut self,
        store: S,
        f: impl Fn(&mut InPlaceNodeSeqBuilder<S>) -> Result<(), S::Error>,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        let mut arc = self.take_arc(&store)?;
        let mut values = Arc::make_mut(&mut arc);
        let mut builder = InPlaceNodeSeqBuilder::<S>::new(&mut values);
        match f(&mut builder) {
            Ok(_) => {
                *values = builder.into_inner();
                Ok(if !values.is_empty() {
                    self.push_new_arc(arc)
                } else {
                    self.push_children_empty()
                })
            }
            Err(cause) => {
                self.move_children();
                Err(cause)
            }
        }
    }

    fn take_arc(&mut self, store: &S) -> Result<Arc<NodeSeqBuilder<S>>, S::Error> {
        let v = self.peek();
        let len = v.bytes().len();
        let res = if let Some(arc) = v.1.arc_as_clone() {
            v.manual_drop();
            arc
        } else if let Some(id) = v.1.id_as_slice() {
            let data = store.read(&id[1..])?;
            Arc::new(NodeSeqBuilder::new(data.as_ref().into()))
        } else {
            Arc::new(NodeSeqBuilder::empty())
        };
        // replace current value with "no children" paceholder
        self.0.s0 += len - 1;
        self.0.vec[self.0.s0] = NONE;
        Ok(res)
    }

    pub fn set_new_arc(
        mut self,
        value: Arc<NodeSeqBuilder<S>>,
    ) -> InPlaceBuilderRef<'a, S, AtChildren> {
        debug_assert!(Arc::strong_count(&value) == 1);
        let t1 = self.0.t1;
        self.drop_current();
        self.0.push_arc(value);
        self.0.rewind(t1);
        self.done()
    }

    fn drop_current(&mut self) {
        let peek = self.peek();
        let len = peek.bytes().len();
        peek.manual_drop();
        self.0.drop(len);
    }

    pub fn push_new_arc(
        mut self,
        value: Arc<NodeSeqBuilder<S>>,
    ) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        debug_assert!(Arc::strong_count(&value) == 1);
        self.drop_current();
        self.0.push_arc(value);
        self.done()
    }

    pub fn push_children_empty(mut self) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.drop_current();
        self.0.push_none();
        self.done()
    }

    pub fn move_children(self) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.0.forward(self.peek().bytes().len());
        self.done()
    }

    pub fn push_id(mut self, id: &[u8]) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.drop_current();
        self.0.push_id(id);
        self.done()
    }

    pub fn attach_children(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        Ok(if self.peek().is_empty() {
            self.move_children()
        } else {
            let mut arc = self.take_arc(store)?;
            let mut b = Arc::make_mut(&mut arc);
            let mut ip = InPlaceNodeSeqBuilder::new(&mut b);
            ip.attach(store)?;
            let data = ip.into_inner();
            let rs = data.compute_record_size();
            let mut id = store.write(&data.data)?;
            id.insert(0, rs as u8);
            self.push_id(&id)
        })
    }

    pub fn detach_children(
        mut self,
        store: &S,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        Ok(if self.peek().is_empty() {
            self.move_children()
        } else {
            let mut arc = self.take_arc(store)?;
            let mut b = Arc::make_mut(&mut arc);
            let mut ip = InPlaceNodeSeqBuilder::new(&mut b);
            ip.detach(store)?;
            *b = ip.into_inner();
            self.push_new_arc(arc)
        })
    }

    pub fn insert_converted<S2: BlobStore>(
        self,
        children: &TreeChildrenRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        // todo fix
        children.manual_clone();
        self.0.extend_from_slice(children.bytes());
        Ok(self.done())
    }
}

pub(crate) struct NodeSeqBuilder<S: BlobStore = NoStore> {
    pub(crate) data: SmallVec<[u8; 16]>,
    pub(crate) record_size: usize,
    p: PhantomData<S>,
}

impl<S: BlobStore> Debug for NodeSeqBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<S: BlobStore> NodeSeqBuilder<S> {
    pub fn blob(&self) -> Blob<'_> {
        Blob::new(self.data.as_ref())
    }

    pub fn as_owned_blob(self: Arc<Self>) -> OwnedBlob {
        let data: &[u8] = self.data.as_ref();
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        OwnedBlob::owned_new(data, Some(self))
    }

    pub fn iter(&self) -> NodeSeqIter<'_, S> {
        NodeSeqIter::new(&self.data)
    }

    pub fn cursor(&mut self) -> BuilderRef<'_, S, AtPrefix> {
        BuilderRef(self, PhantomData)
    }

    pub fn new(data: SmallVec<[u8; 16]>) -> Self {
        Self {
            data,
            record_size: 0,
            p: PhantomData,
        }
    }

    pub fn compute_record_size(&self) -> usize {
        let mut res = 0;
        for node in NodeSeqIter::<S>::new(&self.data) {
            let rs = node.prefix().bytes().len()
                + node.value().bytes().len()
                + node.children().bytes().len();
            if res == 0 {
                res = rs;
            } else if res != rs {
                res = 0;
                break;
            }
        }
        res
    }

    pub fn empty() -> Self {
        Self::new(SmallVec::new())
    }

    pub fn make_non_empty(&mut self) {
        if self.data.is_empty() {
            self.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn into_inner(mut self) -> SmallVec<[u8; 16]> {
        let mut r = SmallVec::new();
        std::mem::swap(&mut self.data, &mut r);
        drop(self);
        r
    }

    pub fn push_non_empty(
        &mut self,
        prefix: impl AsRef<[u8]>,
        value: Option<impl AsRef<[u8]>>,
        children: NodeSeqBuilder<S>,
    ) {
        if !value.is_none() || !children.is_empty() {
            self.cursor()
                .push_prefix(prefix)
                .push_value_opt(value)
                .push_children(children);
            self.record_size = 0;
        }
    }

    pub fn push_detached<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    pub fn push_new_unsplit(
        &mut self,
        prefix: impl AsRef<[u8]>,
        value: Option<impl AsRef<[u8]>>,
        children: NodeSeqBuilder<S>,
        store: &S,
    ) -> Result<(), S::Error> {
        // todo
        Ok(self.push_non_empty(prefix, value, children))
    }

    pub fn push(&mut self, node: &TreeNode<'_, S>) {
        node.manual_clone();
        self.cursor()
            .push_prefix_raw(node.prefix())
            .push_value_raw(node.value())
            .push_children_raw(node.children());
        self.record_size = 0;
    }

    pub fn push_shortened(
        &mut self,
        node: &TreeNode<'_, S>,
        store: &S,
        n: usize,
    ) -> Result<(), S::Error> {
        let cursor = self.cursor();
        let cursor = if n > 0 {
            let prefix = node.prefix().load(store)?;
            assert!(n < prefix.len());
            cursor.push_prefix(&prefix[n..])
        } else {
            node.prefix().manual_clone();
            cursor.push_prefix_raw(node.prefix())
        };
        cursor
            .push_value_raw(node.value())
            .push_children_raw(node.children());
        self.record_size = 0;
        Ok(())
    }

    pub fn push_converted<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    pub fn push_shortened_converted<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
        n: usize,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        t.push_shortened(node, store, n)
    }

    pub fn shortened(node: &TreeNode<'_, S>, store: &S, n: usize) -> Result<Self, S::Error> {
        let mut res = Self::empty();
        res.push_shortened(node, store, n)?;
        Ok(res)
    }

    pub fn single(key: &[u8], value: &[u8]) -> Self {
        let mut t = Self::empty();
        t.data.push_arc_or_inline(key);
        t.data.push_arc_or_inline(value);
        t.data.push_none();
        t.record_size = t.data.len();
        t
    }
}

impl<S: BlobStore> Clone for NodeSeqBuilder<S> {
    fn clone(&self) -> Self {
        for elem in self.iter() {
            elem.manual_clone();
        }
        Self {
            data: self.data.clone(),
            record_size: self.record_size,
            p: PhantomData,
        }
    }
}

impl<S: BlobStore> Drop for NodeSeqBuilder<S> {
    fn drop(&mut self) {
        for elem in self.iter() {
            elem.manual_drop();
        }
        self.data.truncate(0);
    }
}

#[repr(transparent)]
pub(crate) struct InPlaceNodeSeqBuilder<S: BlobStore = NoStore> {
    inner: InPlaceFlexRefSeqBuilder,
    p: PhantomData<S>,
}

impl<S: BlobStore> InPlaceNodeSeqBuilder<S> {
    /// creates an InPlaceNodeSeqBuilder by ripping the inner vec out of a NodeSeqBuilder
    pub fn new(from: &mut NodeSeqBuilder<S>) -> Self {
        if from.is_empty() {
            // insert dummy empty node!
            from.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
        Self {
            inner: InPlaceFlexRefSeqBuilder::new(from.data.take()),
            p: PhantomData,
        }
    }

    pub fn make_non_empty(&mut self) {
        assert!(self.inner.s0 == self.inner.vec.len());
        if self.inner.t1 == 0 {
            self.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
    }

    /// consumes an InPlaceNodeSeqBuilder by storing the result in a NodeSeqBuilder
    pub fn into_inner(mut self) -> NodeSeqBuilder<S> {
        let t = self.inner.take_result();
        // validate_nodeseq_slice::<S>(&t);
        NodeSeqBuilder::new(t)
    }

    /// A cursor, assuming we are currently at the start of a triple
    #[inline]
    pub fn cursor(&mut self) -> InPlaceBuilderRef<'_, S, AtPrefix> {
        InPlaceBuilderRef(&mut self.inner, PhantomData)
    }

    /// Peek element of the source
    pub fn peek(&self) -> Option<u8> {
        if self.inner.has_remaining() {
            Some(TreePrefixRef::<S>::new(FlexRef::new(self.inner.source_slice())).first())
        } else {
            None
        }
    }

    /// move one triple from the source to the target
    pub fn move_one(&mut self) {
        self.cursor().move_prefix().move_value().move_children();
    }

    /// move one triple from the source to the target
    pub fn drop_one(&mut self) {
        let node = TreeNode::<S>::read(self.inner.source_slice()).unwrap();
        node.manual_drop();
        self.inner.s0 += node.prefix().bytes().len()
            + node.value().bytes().len()
            + node.children().bytes().len();
    }

    /// move one triple from the source to the target, canonicalizing
    pub fn move_non_empty(&mut self) {
        let p = self.cursor();
        let start = p.mark();
        let v = p.move_prefix();
        let ve = v.peek().is_none();
        let c = v.move_value();
        let ce = c.peek().is_empty();
        let p1 = c.move_children();
        if ve && ce {
            p1.rewind(start);
        }
    }

    /// converts and inserts! a tree node, without dropping anything
    pub fn insert_converted<S2: BlobStore>(
        &mut self,
        node: TreeNode<S2>,
        store: &S2,
    ) -> Result<(), S::Error> {
        // todo: we must not fail in the middle, since that will leave a mess. Hence the unwrap. Fix this.
        self.cursor()
            .insert_converted(node.prefix(), store)
            .unwrap()
            .insert_converted(node.value(), store)
            .unwrap()
            .insert_converted(node.children(), store)
            .unwrap();
        Ok(())
    }

    pub fn attach(&mut self, store: &S) -> Result<(), S::Error> {
        while !self.inner.source_slice().is_empty() {
            self.cursor()
                .attach_prefix(store)?
                .attach_value(store)?
                .attach_children(store)?;
        }
        Ok(())
    }

    pub fn detach(&mut self, store: &S) -> Result<(), S::Error> {
        while !self.inner.source_slice().is_empty() {
            self.cursor()
                .detach_prefix(store)?
                .detach_value(store)?
                .detach_children(store)?;
        }
        Ok(())
    }
}

impl<S: BlobStore> Drop for InPlaceNodeSeqBuilder<S> {
    fn drop(&mut self) {
        let mut target = FlexRefIter::new(self.inner.target_slice());
        let mut i = 0;
        while let Some(x) = target.next() {
            match i % 3 {
                0 => TreePrefixRef::<S>::new(FlexRef::new(x)).manual_drop(),
                1 => TreeValueOptRef::<S>::new(FlexRef::new(x)).manual_drop(),
                2 => TreeChildrenRef::<S>::new(FlexRef::new(x)).manual_drop(),
                _ => panic!(),
            }
            i += 1;
        }
        if !target.is_empty() {
            return;
        }
        let mut source = FlexRefIter::new(self.inner.source_slice());
        while let Some(x) = source.next() {
            match i % 3 {
                0 => TreePrefixRef::<S>::new(FlexRef::new(x)).manual_drop(),
                1 => TreeValueOptRef::<S>::new(FlexRef::new(x)).manual_drop(),
                2 => TreeChildrenRef::<S>::new(FlexRef::new(x)).manual_drop(),
                _ => panic!(),
            }
            i += 1;
        }
        if !source.is_empty() {
            return;
        }
    }
}
