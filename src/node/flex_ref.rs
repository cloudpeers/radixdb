use std::{cmp::Ordering, fmt, marker::PhantomData, ops::Deref, sync::Arc};

use crate::{
    store::{unwrap_safe, Blob2 as Blob, BlobStore2 as BlobStore, NoError, NoStore, OwnedBlob},
    Hex,
};
use std::fmt::Debug;

trait DataStore: Clone {
    type Error;
    fn read_prefix(self, id: &[u8]) -> Result<(Self, OwnedBlob), Self::Error>;
    fn read(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error>;
    fn write_prefix(self, data: &[u8]) -> Result<(Self, OwnedBlob), Self::Error>;
    fn write(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error>;
    fn sync(&self) -> Result<(), Self::Error>;
}

impl DataStore for NoStore {
    type Error = NoError;

    fn read_prefix(self, id: &[u8]) -> Result<(Self, OwnedBlob), Self::Error> {
        todo!()
    }

    fn read(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error> {
        todo!()
    }

    fn write_prefix(self, data: &[u8]) -> Result<(Self, OwnedBlob), Self::Error> {
        todo!()
    }

    fn write(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error> {
        todo!()
    }

    fn sync(&self) -> Result<(), Self::Error> {
        todo!()
    }
}

#[repr(C)]
struct FlexRef<T>(PhantomData<T>, [u8]);

impl<T> Debug for FlexRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match tpe(self.header()) {
            Type::None => write!(f, "FlexRef::None"),
            Type::Id => write!(f, "FlexRef::Id"),
            Type::Inline => write!(
                f,
                "FlexRef::Inline({})",
                Hex::new(self.inline_as_ref().unwrap())
            ),
            Type::Ptr => write!(f, "FlexRef::Ptr"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Type {
    None,
    Inline,
    Id,
    Ptr,
}

#[repr(transparent)]
struct TreePrefixRef<S = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreePrefixRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreePrefix({:?})", &self.1)
    }
}

impl<S: BlobStore> TreePrefixRef<S> {
    fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    fn empty() -> &'static Self {
        Self::read(&[INLINE_EMPTY]).unwrap()
    }

    fn is_empty(&self) -> bool {
        self.first_opt().is_none()
    }

    fn first_opt(&self) -> Option<u8> {
        self.1.first_u8_opt()
    }

    fn first(&self) -> u8 {
        self.1.first_u8()
    }

    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::None
    }

    fn load(&self, store: &S) -> Result<Blob, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            Blob::new(x)
        } else if let Some(x) = self.1.arc_vec_as_slice() {
            Blob::new(x)
        } else if let Some(id) = self.1.id_as_slice() {
            store.read(id)?
        } else {
            panic!()
        })
    }

    fn read(value: &[u8]) -> Option<&Self> {
        Some(Self::new(FlexRef::read(value)?))
    }

    fn read_one<'a>(value: &'a [u8]) -> Option<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Some((Self::new(f), rest))
    }

    fn manual_drop(&self) {
        self.1.manual_drop();
    }

    fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

#[derive(Debug)]
pub struct TreeValueRefWrapper<S = NoStore>(OwnedBlob, PhantomData<S>);

impl AsRef<[u8]> for TreeValueRefWrapper {
    fn as_ref(&self) -> &[u8] {
        let t = TreeValueRef::<NoStore>::new(FlexRef::new(self.0.as_ref()));
        t.data().unwrap()
    }
}

impl Deref for TreeValueRefWrapper {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[repr(transparent)]
pub struct TreeValueRef<S: BlobStore = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreeValueRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeValue({:?})", &self.1)
    }
}

impl<S: BlobStore> TreeValueRef<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::None
    }

    fn load(&self, store: &S) -> Result<Blob, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            Blob::new(x)
        } else if let Some(x) = self.1.arc_vec_as_slice() {
            Blob::new(x)
        } else if let Some(id) = self.1.id_as_slice() {
            store.read(id)?
        } else {
            panic!()
        })
    }

    fn data(&self) -> Option<&[u8]> {
        self.1.inline_as_ref().or_else(|| self.1.arc_vec_as_slice())
    }
}

impl TreeValueRef {
    pub fn to_owned(&self) -> OwnedBlob {
        unwrap_safe(self.load(&NoStore)).to_owned()
    }
}

#[repr(transparent)]
struct TreeValueOptRef<S: BlobStore = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreeValueOptRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeValueOpt({:?})", &self.1)
    }
}

impl<S: BlobStore> TreeValueOptRef<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn is_valid(&self) -> bool {
        self.bytes().len() > 0
    }

    fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    fn value_opt(&self) -> Option<&TreeValueRef<S>> {
        if self.is_none() {
            None
        } else {
            Some(TreeValueRef::new(&self.1))
        }
    }

    fn none() -> &'static Self {
        Self::new(FlexRef::new(&[NONE]))
    }

    fn is_none(&self) -> bool {
        self.1.is_none()
    }

    fn is_some(&self) -> bool {
        self.1.is_some()
    }

    fn load(&self, store: &S) -> Result<Option<Blob>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            Some(Blob::new(x))
        } else if let Some(x) = self.1.arc_vec_as_slice() {
            Some(Blob::new(x))
        } else if let Some(id) = self.1.id_as_slice() {
            Some(store.read(id)?)
        } else {
            None
        })
    }

    fn read_one<'a>(value: &'a [u8]) -> Option<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Some((Self::new(f), rest))
    }

    fn manual_drop(&self) {
        self.1.manual_drop();
    }

    fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

#[repr(transparent)]
struct TreeChildrenRef<S: BlobStore>(PhantomData<S>, FlexRef<NodeSeqBuilder<S>>);

impl<S: BlobStore> Debug for TreeChildrenRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.1.tpe() {
            Type::Ptr => write!(f, "TreeChildren::Arc({:?})", self.1.arc_as_clone().unwrap()),
            Type::Id => write!(f, "TreeChildren::Id({:?})", self.1.id_as_slice().unwrap()),
            Type::None => write!(f, "TreeChildren::Empty"),
            Type::Inline => write!(f, "TreeChildren::Invalid"),
        }
    }
}

impl<S: BlobStore> TreeChildrenRef<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn empty() -> &'static Self {
        Self::new(FlexRef::new(&[NONE]))
    }

    fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::Inline
    }

    fn read_one<'a>(value: &'a [u8]) -> Option<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Some((Self::new(f), rest))
    }

    fn load_owned(&self, store: &S) -> Result<NodeSeq<'static, S>, S::Error> {
        Ok(if self.1.is_none() {
            NodeSeq::empty()
        } else if let Some(x) = self.1.arc_as_clone() {
            NodeSeq::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_slice() {
            NodeSeq::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }

    fn load(&self, store: &S) -> Result<NodeSeq<'_, S>, S::Error> {
        Ok(if self.1.is_none() {
            NodeSeq::empty()
        } else if let Some(x) = self.1.arc_as_ref() {
            NodeSeq::from_blob(Blob::new(&x.0))
        } else if let Some(id) = self.1.id_as_slice() {
            NodeSeq::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }

    fn is_empty(&self) -> bool {
        self.1.is_none()
    }

    fn manual_drop(&self) {
        self.1.manual_drop();
    }

    fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

/// a newtype wrapper around a blob that contains a valid nodeseq
#[repr(transparent)]
struct NodeSeq<'a, S: BlobStore>(Blob<'a>, PhantomData<S>);

impl<S: BlobStore> NodeSeq<'static, S> {
    fn owned_iter(&self) -> OwnedNodeSeqIter<S> {
        OwnedNodeSeqIter(self.0.clone(), 0, PhantomData)
    }
}

impl<'a, S: BlobStore> NodeSeq<'a, S> {
    fn empty() -> Self {
        Self(Blob::empty(), PhantomData)
    }

    fn from_arc_vec(value: Arc<NodeSeqBuilder<S>>) -> Self {
        let data: &[u8] = value.as_ref().0.as_ref();
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        Self(Blob::owned_new(data, Some(value)), PhantomData)
    }

    fn from_blob(value: Blob<'a>) -> Self {
        Self(value, PhantomData)
    }

    fn iter(&self) -> NodeSeqIter<'_, S> {
        NodeSeqIter::new(&self.0)
    }

    fn find(&self, first: u8) -> Option<TreeNode<'_, S>> {
        // todo: optimize
        for leaf in self.iter() {
            let first_opt = leaf.prefix().first_opt();
            if first_opt == Some(first) {
                // found it
                return Some(leaf);
            } else if first_opt > Some(first) {
                // not going to come anymore
                return None;
            }
        }
        None
    }
}

struct OwnedNodeSeqIter<S: BlobStore>(OwnedBlob, usize, PhantomData<S>);

impl<S: BlobStore> OwnedNodeSeqIter<S> {
    fn new(data: OwnedBlob) -> Self {
        Self(data, 0, PhantomData)
    }

    fn next_node(&mut self) -> Option<TreeNode<'_, S>> {
        if let Some(res) = TreeNode::read(&self.0[self.1..]) {
            // let start = self.0.as_ptr() as usize;
            // let end = rest.as_ptr() as usize;
            // self.1 = end - start;
            self.1 += res.prefix().bytes().len()
                + res.value().bytes().len()
                + res.children().bytes().len();
            Some(res)
        } else {
            None
        }
    }

    fn next_value_and_node(&mut self) -> Option<(Option<OwnedBlob>, TreeNode<'_, S>)> {
        if let Some(res) = TreeNode::read(&self.0[self.1..]) {
            // let start = self.0.as_ptr() as usize;
            // let end = rest.as_ptr() as usize;
            // self.1 = end - start;
            self.1 += res.prefix().bytes().len()
                + res.value().bytes().len()
                + res.children().bytes().len();
            let v = res.value().value_opt().map(|x| self.slice_ref(x.bytes()));
            Some((v, res))
        } else {
            None
        }
    }

    fn slice_ref(&self, slice: &[u8]) -> OwnedBlob {
        self.0.slice_ref(slice)
    }
}

#[repr(transparent)]
struct NodeSeqIter<'a, S>(&'a [u8], PhantomData<S>);

impl<'a, S: BlobStore> Clone for NodeSeqIter<'a, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<'a, S: BlobStore> Copy for NodeSeqIter<'a, S> {}

impl<'a, S: BlobStore> NodeSeqIter<'a, S> {
    fn new(data: &'a [u8]) -> Self {
        Self(data, PhantomData)
    }

    fn peek(&self) -> Option<u8> {
        if self.0.is_empty() {
            None
        } else {
            TreePrefixRef::<S>::read(&self.0).map(|x| x.first())
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn next(&mut self) -> Option<TreeNode<'_, S>> {
        if self.0.is_empty() {
            None
        } else {
            match TreeNode::read_one(&mut self.0) {
                Some((res, rest)) => {
                    self.0 = rest;
                    Some(res)
                }
                None => None,
            }
        }
    }
}

impl<'a, S: BlobStore> Iterator for NodeSeqIter<'a, S> {
    type Item = TreeNode<'a, S>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            match TreeNode::read_one(&mut self.0) {
                Some((res, rest)) => {
                    self.0 = rest;
                    Some(res)
                }
                None => None,
            }
        }
    }
}

pub struct TreeNode<'a, S: BlobStore = NoStore> {
    prefix: &'a TreePrefixRef<S>,
    value: &'a TreeValueOptRef<S>,
    children: &'a TreeChildrenRef<S>,
}

impl<'a, S: BlobStore> std::fmt::Debug for TreeNode<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeNode")
            .field("prefix", &self.prefix())
            .field("value", &self.value())
            .field("children", &self.children())
            .finish()
    }
}

impl<'a, S: BlobStore + 'static> TreeNode<'a, S> {
    fn empty() -> Self {
        TreeNode {
            prefix: TreePrefixRef::empty(),
            value: TreeValueOptRef::none(),
            children: TreeChildrenRef::empty(),
        }
    }

    fn prefix(&self) -> &'a TreePrefixRef<S> {
        &self.prefix
    }

    fn value(&self) -> &'a TreeValueOptRef<S> {
        &self.value
    }

    fn children(&self) -> &'a TreeChildrenRef<S> {
        &self.children
    }

    fn is_empty(&self) -> bool {
        self.children().is_empty() && self.value().is_none()
    }

    fn read(buffer: &'a [u8]) -> Option<Self> {
        Some(Self::read_one(buffer)?.0)
    }

    fn read_one(buffer: &'a [u8]) -> Option<(Self, &'a [u8])> {
        let rest = buffer;
        let (prefix, rest) = TreePrefixRef::<S>::read_one(rest)?;
        let (value, rest) = TreeValueOptRef::<S>::read_one(rest)?;
        let (children, rest) = TreeChildrenRef::<S>::read_one(rest)?;
        Some((
            Self {
                prefix,
                value,
                children,
            },
            rest,
        ))
    }

    fn manual_clone(&self) {
        self.prefix().manual_clone();
        self.value().manual_clone();
        self.children().manual_clone();
    }

    fn manual_drop(&self) {
        self.prefix().manual_drop();
        self.value().manual_drop();
        self.children().manual_drop();
    }

    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error> {
        let spacer = std::iter::repeat(" ").take(indent).collect::<String>();
        let child_ref_count = self.children().1.ref_count();
        let child_count = self.children().load(store)?.iter().count();
        println!("{}TreeNode", spacer);
        println!(
            "{}  prefix={:?} {}",
            spacer,
            self.prefix(),
            self.prefix().1.ref_count()
        );
        println!(
            "{}  value={:?} {}",
            spacer,
            self.value(),
            self.value().1.ref_count()
        );
        println!("{}  children={:?} {}", spacer, child_count, child_ref_count);
        for child in self.children().load(store)?.iter() {
            child.dump(indent + 4, store)?;
        }
        Ok(())
    }

    /// get the first value
    fn first_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.children().is_empty() {
            todo!()
        } else {
            let children = self.children().load(store)?;
            children.iter().next().unwrap().first_value(store)?
        })
    }

    /// get the first entry
    fn first_entry(
        &self,
        store: &S,
        mut prefix: &[u8],
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        // prefix.append(&self.prefix(), store)?;
        todo!();
    }

    /// get the first value
    fn last_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.children().is_empty() {
            todo!()
        } else {
            let children = self.children().load(store)?;
            children.iter().last().unwrap().first_value(store)?
        })
    }

    /// get the first entry
    fn last_entry(
        &self,
        store: &S,
        mut prefix: &[u8],
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        todo!();
    }
}

/// Return the subtree with the given prefix. Will return an empty tree in case there is no match.
fn filter_prefix<S: BlobStore>(
    node: &TreeNode<S>,
    owner: &OwnedBlob,
    store: &S,
    prefix: &[u8],
) -> Result<NodeSeqBuilder<S>, S::Error> {
    find(store, owner, node, prefix, |o, x| {
        Ok(match x {
            FindResult::Found(res) => {
                let mut t = NodeSeqBuilder::new();
                t.cursor()
                    .push_prefix(prefix)
                    .push_value_raw(res.value())
                    .push_children_raw(res.children());
                t
            }
            FindResult::Prefix {
                tree: res,
                matching,
            } => {
                let mut rp = res.prefix().load(store)?.to_vec();
                rp.splice(0..matching, prefix.iter().cloned());
                let mut t = NodeSeqBuilder::new();
                t.cursor()
                    .push_prefix(rp)
                    .push_value_raw(res.value())
                    .push_children_raw(res.children());
                t
            }
            FindResult::NotFound { .. } => NodeSeqBuilder::new(),
        })
    })
}

impl<T> FlexRef<T> {
    fn arc_as_ref(&self) -> Option<&T> {
        self.with_arc(|arc| {
            let t: &T = arc.as_ref();
            unsafe { std::mem::transmute(t) }
        })
    }
}

impl FlexRef<Vec<u8>> {
    fn arc_vec_as_slice(&self) -> Option<&[u8]> {
        self.arc_as_ref().map(|x| x.as_ref())
    }

    fn first_u8_opt(&self) -> Option<u8> {
        match self.tpe() {
            Type::None => None,
            Type::Inline => self.1.get(1).cloned(),
            Type::Ptr => self.with_arc(|x| x.as_ref().get(0).cloned()).unwrap(),
            Type::Id => todo!("pack first byte into id"),
        }
    }

    fn first_u8(&self) -> u8 {
        match self.tpe() {
            Type::Inline => self.1[1],
            Type::Ptr => self.with_arc(|x| x[0]).unwrap(),
            Type::None => panic!(),
            Type::Id => todo!("pack first byte into id"),
        }
    }
}

trait VecTakeExt<T> {
    fn take(&mut self) -> Vec<T>;
}

impl<T> VecTakeExt<T> for Vec<T> {
    fn take(&mut self) -> Vec<T> {
        let mut t = Vec::new();
        std::mem::swap(&mut t, self);
        t
    }
}

impl<T> FlexRef<T> {
    const fn header(&self) -> u8 {
        self.1[0]
    }

    fn data(&self) -> &[u8] {
        let len = len(self.header());
        &self.1[1..len + 1]
    }

    fn bytes(&self) -> &[u8] {
        let len = len(self.header());
        &self.1[0..len + 1]
    }

    fn manual_drop(&self) {
        self.with_arc(|arc| unsafe {
            Arc::decrement_strong_count(Arc::as_ptr(arc));
        });
    }

    fn manual_clone(&self) {
        self.with_arc(|arc| unsafe {
            Arc::increment_strong_count(Arc::as_ptr(arc));
        });
    }

    fn new(value: &[u8]) -> &Self {
        unsafe { std::mem::transmute(value) }
    }

    fn none() -> &'static Self {
        Self::read(&[NONE]).unwrap()
    }

    fn empty() -> &'static Self {
        Self::read(&[INLINE_EMPTY]).unwrap()
    }

    fn is_empty(&self) -> bool {
        self.header() == INLINE_EMPTY
    }

    fn read_one(value: &[u8]) -> Option<(&Self, &[u8])> {
        if value.len() == 0 {
            return None;
        }
        let len = len(value[0]);
        if len + 1 > value.len() {
            return None;
        }
        Some((Self::new(value), &value[len + 1..]))
    }

    fn read(value: &[u8]) -> Option<&Self> {
        Some(Self::read_one(value)?.0)
    }

    const fn tpe(&self) -> Type {
        tpe(self.header())
    }

    fn inline_as_ref(&self) -> Option<&[u8]> {
        if let Type::Inline = self.tpe() {
            Some(self.data())
        } else {
            None
        }
    }

    fn arc_as_clone(&self) -> Option<Arc<T>> {
        self.with_arc(|x| x.clone())
    }

    fn id_as_slice(&self) -> Option<&[u8]> {
        self.with_id(|_| todo!())
    }

    fn ref_count(&self) -> usize {
        self.with_arc(|x| Arc::strong_count(x)).unwrap_or_default()
    }

    fn with_arc<U>(&self, f: impl Fn(&Arc<T>) -> U) -> Option<U> {
        if self.tpe() == Type::Ptr {
            let value = u64::from_be_bytes(self.1[1..9].try_into().unwrap());
            let value = usize::try_from(value).unwrap();
            let arc: Arc<T> = unsafe { std::mem::transmute(value) };
            let res = Some(f(&arc));
            std::mem::forget(arc);
            res
        } else {
            None
        }
    }

    fn with_inline<U>(&self, f: impl Fn(&[u8]) -> U) -> Option<U> {
        if self.tpe() == Type::Inline {
            Some(f(self.data()))
        } else {
            None
        }
    }

    fn with_id<U>(&self, f: impl Fn(u64) -> U) -> Option<U> {
        if self.tpe() == Type::Id {
            let id = u64::from_be_bytes(self.1[1..9].try_into().unwrap());
            Some(f(id))
        } else {
            None
        }
    }

    fn is_none(&self) -> bool {
        self.tpe() == Type::None
    }

    fn is_some(&self) -> bool {
        !self.is_none()
    }
}

const fn len(value: u8) -> usize {
    (value & 0x3f) as usize
}

const fn tpe(value: u8) -> Type {
    match value >> 6 {
        0 => Type::None,
        1 => Type::Inline,
        2 => Type::Ptr,
        3 => Type::Id,
        _ => panic!(),
    }
}

const fn make_header_byte(tpe: Type, len: usize) -> u8 {
    assert!(len < 64);
    (len as u8)
        | match tpe {
            Type::None => 0,
            Type::Inline => 1,
            Type::Ptr => 2,
            Type::Id => 3,
        } << 6
}

const NONE: u8 = make_header_byte(Type::None, 0);
const INLINE_EMPTY: u8 = make_header_byte(Type::Inline, 0);
const PTR8: u8 = make_header_byte(Type::Ptr, 8);

#[derive(Default)]

struct InPlaceFlexRefSeqBuilder {
    // place for the data. [..t1] and [s1..] contain valid sequences of flexrefs.
    // The rest is to be considered uninitialized
    vec: Vec<u8>,
    // end of the result
    t1: usize,
    // start of the source
    s0: usize,
}

impl fmt::Debug for InPlaceFlexRefSeqBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}, {}]",
            Hex::new(self.target_slice()),
            Hex::new(self.source_slice())
        )
    }
}

trait Extendable {
    fn reserve(&mut self, n: usize);

    fn extend_from_slice(&mut self, data: &[u8]);

    fn push(&mut self, value: u8);

    fn push_id(&mut self, id: &[u8]) {
        self.reserve(1 + id.len());
        self.push(make_header_byte(Type::Id, id.len()));
        self.extend_from_slice(id);
    }

    fn push_none(&mut self) {
        self.push(NONE);
    }

    fn push_arc<T>(&mut self, arc: Arc<T>) {
        let data: usize = unsafe { std::mem::transmute(arc) };
        let data: u64 = data as u64;
        self.reserve(9);
        self.push(PTR8);
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
        debug_assert!(data.len() < 64);
        self.reserve(data.len() + 1);
        self.push(make_header_byte(Type::Inline, data.len()));
        self.extend_from_slice(data);
    }
}

impl Extendable for Vec<u8> {
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
            let space = self.vec.capacity() - self.vec.len();
            self.vec
                .splice(self.s0..self.s0, std::iter::repeat(0).take(space));
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
    let mut iter = FlexRefIter(value);
    let mut n = 0;
    while let Some(item) = iter.next() {
        // debug_assert!(!item.is_empty());
        n += 1;
    }
    // debug_assert!(iter.is_empty());
    n
}

fn validate_nodeseq_slice<S: BlobStore>(value: &[u8]) -> usize {
    let mut iter = NodeSeqIter::<S>(value, PhantomData);
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
    pub fn new(vec: Vec<u8>) -> Self {
        Self { vec, t1: 0, s0: 0 }
    }

    fn into_inner(mut self) -> Vec<u8> {
        // debug_assert!(self.source_count() == 0);
        // debug_assert!(self.target_count() % 3 == 0);
        self.vec.truncate(self.t1);
        self.vec
    }

    fn take_result(&mut self) -> Vec<u8> {
        // debug_assert!(self.source_count() == 0);
        // debug_assert!(self.target_count() % 3 == 0);
        self.vec.truncate(self.t1);
        let mut res = Vec::new();
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

struct FlexRefIter<'a>(&'a [u8]);

impl<'a> FlexRefIter<'a> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> Iterator for FlexRefIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            let len = 1 + len(self.0[0]);
            if self.0.len() >= len {
                let res = Some(&self.0[..len]);
                self.0 = &self.0[len..];
                res
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
struct AtPrefix;
#[derive(Debug)]
struct AtValue;
#[derive(Debug)]
struct AtChildren;

trait IterPosition: Debug {}
impl IterPosition for AtPrefix {}
impl IterPosition for AtValue {}
impl IterPosition for AtChildren {}

macro_rules! unwrap_or_break {
    ($res:expr) => {
        match $res {
            Some(val) => val,
            None => {
                break;
            }
        }
    };
}

#[derive(Debug)]
#[repr(transparent)]
struct BuilderRef<'a, S: BlobStore, P: IterPosition>(&'a mut NodeSeqBuilder<S>, PhantomData<P>);

impl<'a, S: BlobStore> BuilderRef<'a, S, AtPrefix> {
    fn push_prefix(self, prefix: impl AsRef<[u8]>) -> BuilderRef<'a, S, AtValue> {
        self.0 .0.push_arc_or_inline(prefix.as_ref());
        BuilderRef(self.0, PhantomData)
    }

    fn push_prefix_raw(self, value: &TreePrefixRef<S>) -> BuilderRef<'a, S, AtValue> {
        value.manual_clone();
        self.0 .0.extend_from_slice(value.bytes());
        BuilderRef(self.0, PhantomData)
    }

    fn push_prefix_empty(mut self) -> BuilderRef<'a, S, AtValue> {
        self.0 .0.push_inline(&[]);
        BuilderRef(self.0, PhantomData)
    }
}

impl<'a, S: BlobStore> BuilderRef<'a, S, AtValue> {
    fn push_value(self, value: impl AsRef<[u8]>) -> BuilderRef<'a, S, AtChildren> {
        self.0 .0.push_arc_or_inline(value.as_ref());
        BuilderRef(self.0, PhantomData)
    }

    fn push_value_opt(self, value: Option<impl AsRef<[u8]>>) -> BuilderRef<'a, S, AtChildren> {
        if let Some(value) = value {
            self.0 .0.push_arc_or_inline(value.as_ref());
        } else {
            self.0 .0.push_none();
        }
        BuilderRef(self.0, PhantomData)
    }

    fn push_value_none(mut self) -> BuilderRef<'a, S, AtChildren> {
        self.0 .0.push_none();
        BuilderRef(self.0, PhantomData)
    }

    fn push_value_raw(self, value: &TreeValueOptRef<S>) -> BuilderRef<'a, S, AtChildren> {
        value.manual_clone();
        self.0 .0.extend_from_slice(value.bytes());
        BuilderRef(self.0, PhantomData)
    }
}

impl<'a, S: BlobStore> BuilderRef<'a, S, AtChildren> {
    fn push_children(self, children: NodeSeqBuilder<S>) -> BuilderRef<'a, S, AtPrefix> {
        if !children.is_empty() {
            self.0 .0.push_arc(Arc::new(children));
        } else {
            self.0 .0.push_none();
        }
        BuilderRef(self.0, PhantomData)
    }

    fn push_children_raw(self, value: &TreeChildrenRef<S>) -> BuilderRef<'a, S, AtPrefix> {
        value.manual_clone();
        self.0 .0.extend_from_slice(value.bytes());
        BuilderRef(self.0, PhantomData)
    }

    fn push_children_empty(self) -> BuilderRef<'a, S, AtChildren> {
        self.0 .0.push_none();
        BuilderRef(self.0, PhantomData)
    }
}

#[derive(Debug)]
#[repr(transparent)]
struct InPlaceBuilderRef<'a, S: BlobStore, P: IterPosition>(
    &'a mut InPlaceFlexRefSeqBuilder,
    PhantomData<(S, P)>,
);

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtPrefix> {
    pub fn peek(&self) -> &TreePrefixRef<S> {
        TreePrefixRef::new(FlexRef::new(self.0.source_slice()))
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
        Ok(self.push_prefix_ref(&prefix[..n]))
    }

    fn push_prefix_empty(mut self) -> InPlaceBuilderRef<'a, S, AtValue> {
        self.drop_current();
        self.0.push_inline(&[]);
        self.done()
    }

    pub fn push_prefix_ref(
        mut self,
        prefix: impl AsRef<[u8]>,
    ) -> InPlaceBuilderRef<'a, S, AtValue> {
        self.drop_current();
        self.0.push_arc_or_inline(prefix.as_ref());
        self.done()
    }

    pub fn insert_converted<S2: BlobStore>(
        mut self,
        prefix: &TreePrefixRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        // todo fix
        self.0.extend_from_slice(prefix.bytes());
        Ok(self.done())
    }

    fn drop_current(&mut self) {
        let peek = self.peek();
        let len = peek.bytes().len();
        peek.manual_drop();
        self.0.drop(len);
    }

    fn mark(&self) -> usize {
        self.0.t1
    }

    fn rewind(self, to: usize) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.0.rewind(to);
        InPlaceBuilderRef(self.0, PhantomData)
    }

    fn done(self) -> InPlaceBuilderRef<'a, S, AtValue> {
        InPlaceBuilderRef(self.0, PhantomData)
    }
}

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtValue> {
    fn done(self) -> InPlaceBuilderRef<'a, S, AtChildren> {
        InPlaceBuilderRef(self.0, PhantomData)
    }

    fn drop_current(&mut self) {
        let len = self.peek().bytes().len();
        self.peek().manual_drop();
        self.0.drop(len);
    }

    pub fn peek(&self) -> &TreeValueOptRef<S> {
        TreeValueOptRef::new(FlexRef::new(self.0.source_slice()))
    }

    pub fn move_value(self) -> InPlaceBuilderRef<'a, S, AtChildren> {
        let len = self.peek().bytes().len();
        self.0.forward(len);
        self.done()
    }

    fn push_value_none(mut self) -> InPlaceBuilderRef<'a, S, AtChildren> {
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

    fn push_value(mut self, value: &[u8]) -> InPlaceBuilderRef<'a, S, AtChildren> {
        self.drop_current();
        self.0.push_arc_or_inline(value);
        self.done()
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
        TreeChildrenRef::new(FlexRef::new(self.0.source_slice()))
    }

    pub fn mutate(
        mut self,
        store: S,
        f: impl Fn(&mut InPlaceNodeSeqBuilder<S>) -> Result<(), S::Error>,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        // MUTATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        } else {
            // todo: load data if needed
            Arc::new(NodeSeqBuilder::new())
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
        InPlaceBuilderRef(self.0, PhantomData)
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

    fn done(self) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        InPlaceBuilderRef(self.0, PhantomData)
    }

    pub fn move_children(self) -> InPlaceBuilderRef<'a, S, AtPrefix> {
        self.0.forward(self.peek().bytes().len());
        self.done()
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

#[repr(transparent)]
struct InPlaceNodeSeqBuilder<S: BlobStore = NoStore> {
    inner: InPlaceFlexRefSeqBuilder,
    p: PhantomData<S>,
}

impl<S: BlobStore> InPlaceNodeSeqBuilder<S> {
    /// creates an InPlaceNodeSeqBuilder by ripping the inner vec out of a NodeSeqBuilder
    fn new(from: &mut NodeSeqBuilder<S>) -> Self {
        if from.is_empty() {
            // insert dummy empty node!
            from.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
        Self {
            inner: InPlaceFlexRefSeqBuilder::new(from.0.take()),
            p: PhantomData,
        }
    }

    fn make_non_empty(&mut self) {
        assert!(self.inner.s0 == self.inner.vec.len());
        if self.inner.t1 == 0 {
            self.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
    }

    /// consumes an InPlaceNodeSeqBuilder by storing the result in a NodeSeqBuilder
    fn into_inner(mut self) -> NodeSeqBuilder<S> {
        let t = self.inner.take_result();
        // validate_nodeseq_slice::<S>(&t);
        NodeSeqBuilder(t, PhantomData)
    }

    /// A cursor, assuming we are currently at the start of a triple
    #[inline]
    fn cursor(&mut self) -> InPlaceBuilderRef<'_, S, AtPrefix> {
        InPlaceBuilderRef(&mut self.inner, PhantomData)
    }

    /// Peek element of the source
    fn peek(&self) -> Option<u8> {
        if self.inner.has_remaining() {
            Some(TreePrefixRef::<S>::new(FlexRef::new(self.inner.source_slice())).first())
        } else {
            None
        }
    }

    /// move one triple from the source to the target
    fn move_one(&mut self) {
        self.cursor().move_prefix().move_value().move_children();
    }

    /// move one triple from the source to the target
    fn drop_one(&mut self) {
        let node = TreeNode::<S>::read(self.inner.source_slice()).unwrap();
        node.manual_drop();
        self.inner.s0 += node.prefix().bytes().len()
            + node.value().bytes().len()
            + node.children().bytes().len();
    }

    /// move one triple from the source to the target, canonicalizing
    fn move_non_empty(&mut self) {
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
    fn insert_converted<S2: BlobStore>(
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
}

impl<S: BlobStore> Drop for InPlaceNodeSeqBuilder<S> {
    fn drop(&mut self) {
        let mut target = FlexRefIter(self.inner.target_slice());
        let mut i = 0;
        while let Some(x) = target.next() {
            match i % 3 {
                0 => TreePrefixRef::<S>::new(FlexRef::new(x)).manual_drop(),
                1 => TreeValueOptRef::<S>::new(FlexRef::new(x)).manual_drop(),
                2 => TreeChildrenRef::<S>::new(FlexRef::new(x)).manual_drop(),
                _ => panic!(),
            }
        }
        if !target.0.is_empty() {
            return;
        }
        let mut source = FlexRefIter(self.inner.source_slice());
        while let Some(x) = source.next() {
            match i % 3 {
                0 => TreePrefixRef::<S>::new(FlexRef::new(x)).manual_drop(),
                1 => TreeValueOptRef::<S>::new(FlexRef::new(x)).manual_drop(),
                2 => TreeChildrenRef::<S>::new(FlexRef::new(x)).manual_drop(),
                _ => panic!(),
            }
        }
        if !source.0.is_empty() {
            return;
        }
    }
}

struct TreeNodeMut<'a, S: BlobStore = NoStore>(&'a mut InPlaceNodeSeqBuilder<S>, TreeNode<'a, S>);

impl<'a, S: BlobStore> AsRef<TreeNode<'a, S>> for TreeNodeMut<'a, S> {
    fn as_ref(&self) -> &TreeNode<'a, S> {
        &self.1
    }
}

impl<'a, S: BlobStore> Deref for TreeNodeMut<'a, S> {
    type Target = TreeNode<'a, S>;

    fn deref(&self) -> &TreeNode<'a, S> {
        &self.as_ref()
    }
}

struct NodeSeqBuilder<S: BlobStore = NoStore>(Vec<u8>, PhantomData<S>);

impl<S: BlobStore> Debug for NodeSeqBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<S: BlobStore> NodeSeqBuilder<S> {
    fn as_owned_blob(self: Arc<Self>) -> OwnedBlob {
        let data: &[u8] = self.0.as_ref();
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        OwnedBlob::owned_new(data, Some(self))
    }

    fn iter(&self) -> NodeSeqIter<'_, S> {
        NodeSeqIter(&self.0, PhantomData)
    }

    fn cursor(&mut self) -> BuilderRef<'_, S, AtPrefix> {
        BuilderRef(self, PhantomData)
    }

    fn new() -> Self {
        Self(Vec::with_capacity(32), PhantomData)
    }

    fn make_non_empty(&mut self) {
        if self.0.is_empty() {
            self.cursor()
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn into_inner(mut self) -> Vec<u8> {
        let mut r = Vec::new();
        std::mem::swap(&mut self.0, &mut r);
        drop(self);
        r
    }

    fn empty_tree() -> Self {
        let mut res = InPlaceFlexRefSeqBuilder::default();
        res.push_arc_or_inline(&[]);
        res.push_none();
        res.push_none();
        Self(res.into_inner(), PhantomData)
    }

    fn push_non_empty(
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
        }
    }

    fn push_detached<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    fn push_new_unsplit(
        &mut self,
        prefix: impl AsRef<[u8]>,
        value: Option<impl AsRef<[u8]>>,
        children: NodeSeqBuilder<S>,
        store: &S,
    ) -> Result<(), S::Error> {
        // todo
        Ok(self.push_non_empty(prefix, value, children))
    }

    fn push(&mut self, node: &TreeNode<'_, S>) {
        self.cursor()
            .push_prefix_raw(node.prefix())
            .push_value_raw(node.value())
            .push_children_raw(node.children());
    }

    fn push_shortened(
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
            cursor.push_prefix_raw(node.prefix())
        };
        cursor
            .push_value_raw(node.value())
            .push_children_raw(node.children());
        Ok(())
    }

    fn push_converted<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    fn push_shortened_converted<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
        n: usize,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        t.push_shortened(node, store, n)
    }

    fn shortened(node: &TreeNode<'_, S>, store: &S, n: usize) -> Result<Self, S::Error> {
        let mut res = Self(Vec::new(), PhantomData);
        res.push_shortened(node, store, n)?;
        Ok(res)
    }

    fn single(key: &[u8], value: &[u8]) -> Self {
        let mut t = InPlaceFlexRefSeqBuilder::default();
        t.push_arc_or_inline(key);
        t.push_arc_or_inline(value);
        t.push_none();
        Self(t.into_inner(), PhantomData)
    }
}

impl<S: BlobStore> Clone for NodeSeqBuilder<S> {
    fn clone(&self) -> Self {
        for elem in self.iter() {
            elem.manual_clone();
        }
        Self(self.0.clone(), PhantomData)
    }
}

impl<S: BlobStore> Drop for NodeSeqBuilder<S> {
    fn drop(&mut self) {
        for elem in self.iter() {
            elem.manual_drop();
        }
        self.0.truncate(0);
    }
}

#[derive(Debug, Clone)]
pub struct Tree<S: BlobStore = NoStore> {
    /// This contains exactly one node, even in the case of an empty tree
    node: Arc<NodeSeqBuilder<S>>,
    /// The associated store
    store: S,
}

impl Tree {
    fn empty() -> Self {
        Self::new(NodeSeqBuilder::new(), NoStore)
    }

    fn single(key: &[u8], value: &[u8]) -> Self {
        Self::new(NodeSeqBuilder::single(key, value), NoStore)
    }

    pub fn iter(&self) -> impl Iterator<Item = (OwnedBlob, TreeValueRefWrapper)> {
        self.try_iter().map(unwrap_safe)
    }

    pub fn values(&self) -> impl Iterator<Item = TreeValueRefWrapper> {
        self.try_values().map(unwrap_safe)
    }

    pub fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> impl Iterator<Item = (OwnedBlob, TreeValueRefWrapper)> + '_ {
        unwrap_safe(self.try_scan_prefix(prefix)).map(unwrap_safe)
    }

    pub fn get(&self, key: &[u8]) -> Option<TreeValueRefWrapper> {
        unwrap_safe(self.try_get(key))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        unwrap_safe(self.try_contains_key(key))
    }

    pub fn first_value(&self) -> Option<TreeValueRefWrapper> {
        unwrap_safe(self.try_first_value())
    }

    pub fn last_value(&self) -> Option<TreeValueRefWrapper> {
        unwrap_safe(self.try_last_value())
    }

    pub fn first_entry(&self, prefix: &[u8]) -> Option<(OwnedBlob, TreeValueRefWrapper)> {
        unwrap_safe(self.try_first_entry(prefix))
    }

    pub fn last_entry(&self, prefix: &[u8]) -> Option<(OwnedBlob, TreeValueRefWrapper)> {
        unwrap_safe(self.try_last_entry(prefix))
    }

    pub fn filter_prefix(&self, prefix: &[u8]) -> Tree {
        unwrap_safe(self.try_filter_prefix(prefix))
    }

    pub fn outer_combine(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_outer_combine::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn outer_combine_with(
        &mut self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) {
        unwrap_safe(self.try_outer_combine_with::<NoStore, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn inner_combine(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_inner_combine::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn inner_combine_pred(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> bool + Copy,
    ) -> bool {
        unwrap_safe(self.try_inner_combine_pred::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn inner_combine_with(
        &mut self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) {
        unwrap_safe(self.try_inner_combine_with::<NoStore, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn left_combine(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_left_combine::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn left_combine_pred(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> bool + Copy,
    ) -> bool {
        unwrap_safe(self.try_left_combine_pred::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn left_combine_with(
        &mut self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<OwnedBlob> + Copy,
    ) {
        unwrap_safe(self.try_left_combine_with::<NoStore, _>(that, |a, b| Ok(f(a, b))))
    }

    pub fn group_by<'a>(
        &'a self,
        f: impl Fn(&[u8], &TreeNode) -> bool + 'a,
    ) -> impl Iterator<Item = Tree> + 'a {
        self.try_group_by(f).map(unwrap_safe)
    }

    pub fn retain_prefix_with(&mut self, that: &Tree, f: impl Fn(&TreeValueRef) -> bool + Copy) {
        unwrap_safe(self.try_retain_prefix_with(that, |b| Ok(f(b))))
    }

    pub fn remove_prefix_with(&mut self, that: &Tree, f: impl Fn(&TreeValueRef) -> bool + Copy) {
        unwrap_safe(self.try_remove_prefix_with(that, |b| Ok(f(b))))
    }
}

impl<S: BlobStore> Tree<S> {
    fn node(&self) -> TreeNode<'_, S> {
        TreeNode::read(&self.node.0).unwrap()
    }

    fn new(mut node: NodeSeqBuilder<S>, store: S) -> Self {
        node.make_non_empty();
        Self {
            node: Arc::new(node),
            store,
        }
    }
}

impl<S: BlobStore + Clone> Tree<S> {
    fn dump(&self) -> Result<(), S::Error> {
        self.node().dump(0, &self.store)
    }

    pub fn try_iter(&self) -> Iter<S> {
        Iter::new(self.owned_iter(), self.store.clone(), IterKey::default())
    }

    pub fn try_values(&self) -> Values<S> {
        Values::new(self.owned_iter(), self.store.clone())
    }

    pub fn try_scan_prefix(&self, prefix: &[u8]) -> Result<Iter<S>, S::Error> {
        scan_prefix(self.store.clone(), &self.owned_blob(), &self.node(), prefix)
    }

    pub fn try_group_by<'a, F: Fn(&[u8], &TreeNode<S>) -> bool + 'a>(
        &'a self,
        descend: F,
    ) -> impl Iterator<Item = Result<Tree<S>, S::Error>> + 'a {
        GroupBy::new(
            self.owned_iter(),
            self.store.clone(),
            IterKey::default(),
            descend,
        )
        .map(|r| {
            r.map(|r| Tree {
                node: Arc::new(r),
                store: self.store.clone(),
            })
        })
    }

    fn owned_blob(&self) -> OwnedBlob {
        self.node.clone().as_owned_blob()
    }

    fn owned_iter(&self) -> OwnedNodeSeqIter<S> {
        OwnedNodeSeqIter::new(self.owned_blob())
    }

    /// Get the value for a given key
    fn try_get(&self, key: &[u8]) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(
            &self.store,
            &self.owned_blob(),
            &self.node(),
            key,
            |o, r| {
                Ok(if let FindResult::Found(tree) = r {
                    tree.value()
                        .value_opt()
                        .map(|x| TreeValueRefWrapper(o.slice_ref(x.bytes()), PhantomData))
                } else {
                    None
                })
            },
        )
    }

    /// True if key is contained in this set
    fn try_contains_key(&self, key: &[u8]) -> Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(
            &self.store,
            &self.owned_blob(),
            &self.node(),
            key,
            |_, r| {
                Ok(if let FindResult::Found(tree) = r {
                    tree.value().is_some()
                } else {
                    false
                })
            },
        )
    }

    pub fn try_outer_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, E> + Copy,
    {
        let mut nodes = NodeSeqBuilder::new();
        outer_combine(
            &self.node(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
            &mut nodes,
        )?;
        Ok(Tree::new(nodes, NoStore))
    }

    pub fn try_outer_combine_with<S2, F>(&mut self, that: &Tree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        S::Error: From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, S::Error> + Copy,
    {
        let node = Arc::make_mut(&mut self.node);
        let mut iter = InPlaceNodeSeqBuilder::new(node);
        outer_combine_with(
            iter.cursor(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )?;
        iter.make_non_empty();
        *node = iter.into_inner();
        Ok(())
    }

    pub fn try_inner_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, E> + Copy,
    {
        let mut nodes = NodeSeqBuilder::new();
        inner_combine(
            &self.node(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
            &mut nodes,
        )?;
        Ok(Tree::new(nodes, NoStore))
    }

    pub fn try_inner_combine_pred<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<bool, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<bool, E> + Copy,
    {
        inner_combine_pred(
            &self.node(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )
    }

    pub fn try_inner_combine_with<S2, F>(&mut self, that: &Tree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        S::Error: From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, S::Error> + Copy,
    {
        let node = Arc::make_mut(&mut self.node);
        let mut iter = InPlaceNodeSeqBuilder::new(node);
        inner_combine_with(
            iter.cursor(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )?;
        iter.make_non_empty();
        *node = iter.into_inner();
        Ok(())
    }

    pub fn try_left_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, E> + Copy,
    {
        let mut nodes = NodeSeqBuilder::new();
        left_combine(
            &self.node(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
            &mut nodes,
        )?;
        Ok(Tree::new(nodes, NoStore))
    }

    pub fn try_left_combine_pred<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<bool, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<bool, E> + Copy,
    {
        left_combine_pred(
            &self.node(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )
    }

    pub fn try_left_combine_with<S2, F>(&mut self, that: &Tree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        S::Error: From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<OwnedBlob>, S::Error> + Copy,
    {
        let node = Arc::make_mut(&mut self.node);
        let mut iter = InPlaceNodeSeqBuilder::new(node);
        left_combine_with(
            iter.cursor(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )?;
        iter.make_non_empty();
        *node = iter.into_inner();
        Ok(())
    }

    pub fn try_retain_prefix_with<S2, F>(&mut self, that: &Tree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        S::Error: From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S2>) -> Result<bool, S2::Error> + Copy,
    {
        let node = Arc::make_mut(&mut self.node);
        let mut iter = InPlaceNodeSeqBuilder::new(node);
        retain_prefix_with(
            iter.cursor(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )?;
        iter.make_non_empty();
        *node = iter.into_inner();
        Ok(())
    }

    pub fn try_remove_prefix_with<S2, F>(&mut self, that: &Tree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        S::Error: From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S2>) -> Result<bool, S2::Error> + Copy,
    {
        let node = Arc::make_mut(&mut self.node);
        let mut iter = InPlaceNodeSeqBuilder::new(node);
        remove_prefix_with(
            iter.cursor(),
            self.store.clone(),
            &that.node(),
            that.store.clone(),
            f,
        )?;
        iter.make_non_empty();
        *node = iter.into_inner();
        Ok(())
    }

    pub fn try_first_value(&self) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        self.node().first_value(&self.store)
    }

    pub fn try_last_value(&self) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        self.node().last_value(&self.store)
    }

    pub fn try_first_entry(
        &self,
        prefix: &[u8],
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        self.node().first_entry(&self.store, prefix)
    }

    pub fn try_last_entry(
        &self,
        prefix: &[u8],
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        self.node().last_entry(&self.store, prefix)
    }

    pub fn try_filter_prefix<'a>(&'a self, prefix: &[u8]) -> Result<Tree<S>, S::Error> {
        filter_prefix(&self.node(), &self.owned_blob(), &self.store, prefix).map(|node| Tree {
            node: Arc::new(node),
            store: self.store.clone(),
        })
    }
}

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

fn outer_combine<A, B, E, F>(
    a: &TreeNode<A>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
    target: &mut NodeSeqBuilder,
) -> Result<(), E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let ap = a.prefix().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let prefix = &ap[..n];
    let mut children: NodeSeqBuilder = NodeSeqBuilder::new();
    let value: Option<Blob>;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = match (a.value().value_opt(), b.value().value_opt()) {
            (Some(a), Some(b)) => f(a, b)?,
            (Some(a), None) => Some(a.load(&ab)?),
            (None, Some(b)) => Some(b.load(&bb)?),
            (None, None) => None,
        };
        let ac = a.children().load(&ab)?;
        let bc = b.children().load(&bb)?;
        children = outer_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = a.value().load(&ab)?;
        let ac = a.children().load(&ab)?;
        let bc = NodeSeqBuilder::shortened(b, &bb, n)?;
        children = outer_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = b.value().load(&bb)?;
        let ac = NodeSeqBuilder::shortened(a, &ab, n)?;
        let bc = b.children().load(&bb)?;
        children = outer_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = None;
        // children is just the shortened children a and b in the right order
        if ap[n] <= bp[n] {
            children.push_shortened_converted(a, &ab, n)?;
            children.push_shortened_converted(b, &bb, n)?;
        } else {
            children.push_shortened_converted(b, &bb, n)?;
            children.push_shortened_converted(a, &ab, n)?;
        }
    }
    target.push_new_unsplit(prefix, value, children, &NoStore)?;
    Ok(())
}

fn outer_combine_children<'a, A, B, E, F>(
    a: NodeSeqIter<'a, A>,
    ab: A,
    b: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<NodeSeqBuilder, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let mut res = NodeSeqBuilder::new();
    let mut iter = OuterJoin::<A, B, E>::new(a, b);
    while let Some(x) = iter.next() {
        match x? {
            (Some(a), Some(b)) => {
                outer_combine(&a, ab.clone(), &b, bb.clone(), f, &mut res)?;
            }
            (Some(a), None) => {
                res.push_detached(&a, &ab)?;
            }
            (None, Some(b)) => {
                res.push_detached(&b, &bb)?;
            }
            (None, None) => {}
        }
    }
    Ok(res)
}

struct OuterJoin<'a, A: BlobStore, B: BlobStore, E> {
    a: NodeSeqIter<'a, A>,
    b: NodeSeqIter<'a, B>,
    p: PhantomData<E>,
}

impl<'a, A, B, E> OuterJoin<'a, A, B, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error>,
{
    pub fn new(a: NodeSeqIter<'a, A>, b: NodeSeqIter<'a, B>) -> Self {
        Self {
            a,
            b,
            p: PhantomData,
        }
    }

    // get the next a, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_a(&mut self) -> Option<(Option<TreeNode<'_, A>>, Option<TreeNode<'_, B>>)> {
        let a = self.a.next().unwrap();
        Some((Some(a), None))
    }

    // get the next b, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_b(&mut self) -> Option<(Option<TreeNode<'_, A>>, Option<TreeNode<'_, B>>)> {
        let b = self.b.next().unwrap();
        Some((None, Some(b)))
    }

    // get the next a and b, assuming we know already that neither is an Err
    #[allow(clippy::type_complexity)]
    fn next_ab(&mut self) -> Option<(Option<TreeNode<'_, A>>, Option<TreeNode<'_, B>>)> {
        let a = self.a.next();
        let b = self.b.next();
        Some((a, b))
    }

    fn next0(&mut self) -> Result<Option<(Option<TreeNode<'_, A>>, Option<TreeNode<'_, B>>)>, E> {
        Ok(
            if let (Some(ak), Some(bk)) = (self.a.peek(), self.b.peek()) {
                match ak.cmp(&bk) {
                    Ordering::Less => self.next_a(),
                    Ordering::Greater => self.next_b(),
                    Ordering::Equal => self.next_ab(),
                }
            } else if let Some(a) = self.a.next() {
                Some((Some(a), None))
            } else if let Some(b) = self.b.next() {
                Some((None, Some(b)))
            } else {
                None
            },
        )
    }

    fn next(&mut self) -> Option<Result<(Option<TreeNode<'_, A>>, Option<TreeNode<'_, B>>), E>> {
        self.next0().transpose()
    }
}

struct Combiner<'a, A: BlobStore, B: BlobStore> {
    a: &'a mut InPlaceNodeSeqBuilder<A>,
    b: NodeSeqIter<'a, B>,
}

impl<'a, A, B> Combiner<'a, A, B>
where
    A: BlobStore,
    B: BlobStore,
    A::Error: From<B::Error>,
{
    pub fn new(a: &'a mut InPlaceNodeSeqBuilder<A>, b: NodeSeqIter<'a, B>) -> Self {
        Self { a, b }
    }

    /// Compare the first element of the lhs builder (a) with the first element of the rhs iterator (b)
    ///
    /// If this methond returns with success, it is guaranteed that neither a nor b produce an error
    fn cmp(&self) -> Result<Option<Ordering>, A::Error> {
        Ok(match (self.a.peek(), self.b.peek()) {
            (Some(a), Some(b)) => Some(a.cmp(&b)),
            (Some(_), None) => Some(Ordering::Less),
            (None, Some(_)) => Some(Ordering::Greater),
            (None, None) => None,
        })
    }
}

fn outer_combine_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    let ap = a.peek().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        // move prefix and value
        let a = a.move_prefix();
        let a = match (a.peek().value_opt(), b.value.value_opt()) {
            (Some(av), Some(bv)) => {
                let r = f(av, bv)?;
                a.push_value_opt(r)
            }
            (Some(_), None) => a.move_value(),
            (None, _) => a.push_converted(b.value, &bb)?,
        };
        let bc = b.children().load(&bb)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        let a = a.move_prefix().move_value();
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::new();
        let cursor = child.cursor();
        // store the last part of the prefix
        let cursor = cursor.push_prefix(&ap[n..]);
        // store the first part of the prefix
        let a = a.move_prefix_shortened(n, &ab)?;
        // store value from a in child, if it exists
        let cursor = cursor.push_value_raw(a.peek());
        // store the value from b, if it exists
        let a = a.push_converted(b.value(), &bb)?;
        // take the children
        let _ = cursor.push_children_raw(a.peek());
        let a = a.set_new_arc(Arc::new(child));
        let bc = b.children().load(&bb)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        let a_le_b = ap[n] <= bp[n];
        let mut child = NodeSeqBuilder::<A>::new();
        let cursor = child.cursor();
        let cursor = cursor.push_prefix(&ap[n..]);
        let a = a.move_prefix_shortened(n, &ab)?;
        let cursor = cursor.push_value_raw(a.peek());
        let a = a.push_value_none();
        let _ = cursor.push_children_raw(a.peek());
        let mut children = NodeSeqBuilder::<A>::new();
        // children is just the shortened children a and b in the right order
        if a_le_b {
            children.push(&child.iter().next().unwrap());
            children.push_shortened_converted(b, &bb, n)?;
        } else {
            children.push_shortened_converted(b, &bb, n)?;
            children.push(&child.iter().next().unwrap());
        }
        a.push_new_arc(Arc::new(children));
    }
    Ok(())
}

fn outer_combine_children_with<'a, A, B, F>(
    a: InPlaceBuilderRef<'a, A, AtChildren>,
    ab: A,
    bc: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<InPlaceBuilderRef<'a, A, AtPrefix>, A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    if bc.is_empty() {
        Ok(a.move_children())
    } else if a.peek().is_empty() {
        let mut res = NodeSeqBuilder::new();
        let mut bc = bc;
        while let Some(b) = bc.next() {
            res.push_converted(&b, &bb)?;
        }
        Ok(a.push_new_arc(Arc::new(res)))
    } else {
        a.mutate(ab.clone(), move |ac| {
            let mut c = Combiner::<A, B>::new(ac, bc);
            while let Some(ordering) = c.cmp()? {
                match ordering {
                    Ordering::Less => {
                        c.a.move_one();
                    }
                    Ordering::Equal => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        let start = c.a.cursor().mark();
                        outer_combine_with(c.a.cursor(), ab.clone(), &b, bb.clone(), f)?;
                        c.a.cursor().rewind(start);
                        // only move if the child is non-empty
                        c.a.move_non_empty();
                    }
                    Ordering::Greater => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        c.a.insert_converted(b, &bb)?;
                    }
                }
            }
            Ok(())
        })
    }
}

fn inner_combine<A, B, E, F>(
    a: &TreeNode<A>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
    target: &mut NodeSeqBuilder,
) -> Result<(), E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let ap = a.prefix().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let prefix = &ap[..n];
    let mut children: NodeSeqBuilder = NodeSeqBuilder::new();
    let value: Option<Blob>;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if let (Some(a), Some(b)) = (a.value().value_opt(), b.value().value_opt()) {
            f(a, b)?
        } else {
            None
        };
        let ac = a.children().load(&ab)?;
        let bc = b.children().load(&bb)?;
        children = inner_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is none
        value = None;
        let ac = a.children().load(&ab)?;
        let bc = NodeSeqBuilder::shortened(b, &bb, n)?;
        children = inner_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is none
        value = None;
        let ac = NodeSeqBuilder::shortened(a, &ab, n)?;
        let bc = b.children().load(&bb)?;
        children = inner_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = None;
    }
    target.push_new_unsplit(prefix, value, children, &NoStore)?;
    Ok(())
}

fn inner_combine_children<'a, A, B, E, F>(
    a: NodeSeqIter<'a, A>,
    ab: A,
    b: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<NodeSeqBuilder, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let mut res = NodeSeqBuilder::new();
    let mut iter = OuterJoin::<A, B, E>::new(a, b);
    while let Some(x) = iter.next() {
        if let (Some(a), Some(b)) = x? {
            inner_combine(&a, ab.clone(), &b, bb.clone(), f, &mut res)?;
        }
    }
    Ok(res)
}

fn inner_combine_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    let ap = a.peek().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        let a = a.move_prefix();
        let a = match (a.peek().value_opt(), b.value.value_opt()) {
            (Some(av), Some(bv)) => {
                let r = f(av, bv)?;
                a.push_value_opt(r)
            }
            _ => a.push_value_none(),
        };
        let bc = b.children().load(&bb)?;
        inner_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        let a = a.move_prefix().push_value_none();
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        inner_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::new();
        let cursor = child.cursor();
        // store the last part of the prefix
        let cursor = cursor.push_prefix(&ap[n..]);
        // store the first part of the prefix
        let a = a.move_prefix_shortened(n, &ab)?;
        // store value from a in child, if it exists
        let cursor = cursor.push_value_raw(a.peek());
        // store the value from b, if it exists
        let a = a.push_value_none();
        // take the children
        let _ = cursor.push_children_raw(a.peek());
        let a = a.set_new_arc(Arc::new(child));
        let bc = b.children().load(&bb)?;
        inner_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint, nothing to do
        a.push_prefix_empty()
            .push_value_none()
            .push_children_empty();
    }
    Ok(())
}

fn inner_combine_children_with<'a, A, B, F>(
    a: InPlaceBuilderRef<'a, A, AtChildren>,
    ab: A,
    bc: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<InPlaceBuilderRef<'a, A, AtPrefix>, A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    if bc.is_empty() {
        Ok(a.push_children_empty())
    } else if a.peek().is_empty() {
        Ok(a.push_children_empty())
    } else {
        a.mutate(ab.clone(), move |ac| {
            let mut c = Combiner::<A, B>::new(ac, bc);
            while let Some(ordering) = c.cmp()? {
                match ordering {
                    Ordering::Less => {
                        // drop one from a
                        c.a.drop_one();
                    }
                    Ordering::Equal => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        let start = c.a.cursor().mark();
                        inner_combine_with(c.a.cursor(), ab.clone(), &b, bb.clone(), f)?;
                        c.a.cursor().rewind(start);
                        // only move if the child is non-empty
                        c.a.move_non_empty();
                    }
                    Ordering::Greater => {
                        // skip one from b
                        let _ = c.b.next();
                    }
                }
            }
            Ok(())
        })
    }
}

/// Inner combine two trees with a predicate f
fn inner_combine_pred<A, B, E, F>(
    a: &TreeNode<A>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<bool, E> + Copy,
{
    let ap = a.prefix.load(&ab)?;
    let bp = b.prefix.load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        if let (Some(av), Some(bv)) = (a.value.value_opt(), b.value.value_opt()) {
            if f(av, bv)? {
                return Ok(true);
            }
        };
        let ac = a.children.load(&ab)?;
        let bc = b.children.load(&bb)?;
        inner_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == ap.len() {
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        let ac = a.children.load(&ab)?;
        inner_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == bp.len() {
        let mut ac = NodeSeqBuilder::<A>::new();
        ac.push_shortened(a, &ab, n)?;
        let bc = b.children.load(&bb)?;
        inner_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else {
        Ok(false)
    }
}

fn inner_combine_children_pred<'a, A, B, E, F>(
    a: NodeSeqIter<'a, A>,
    ab: A,
    b: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<bool, E> + Copy,
{
    let mut iter = OuterJoin::<A, B, E>::new(a, b);
    while let Some(x) = iter.next() {
        if let (Some(a), Some(b)) = x? {
            if inner_combine_pred(&a, ab.clone(), &b, bb.clone(), f)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn left_combine<A, B, E, F>(
    a: &TreeNode<A>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
    target: &mut NodeSeqBuilder,
) -> Result<(), E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let ap = a.prefix().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let prefix = &ap[..n];
    let mut children: NodeSeqBuilder = NodeSeqBuilder::new();
    let value: Option<Blob>;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = match (a.value().value_opt(), b.value().value_opt()) {
            (Some(a), Some(b)) => f(a, b)?,
            (Some(a), None) => Some(a.load(&ab)?),
            _ => None,
        };
        let ac = a.children().load(&ab)?;
        let bc = b.children().load(&bb)?;
        children = left_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = a.value().load(&ab)?;
        let ac = a.children().load(&ab)?;
        let bc = NodeSeqBuilder::shortened(b, &bb, n)?;
        children = left_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = None;
        let ac = NodeSeqBuilder::shortened(a, &ab, n)?;
        let bc = b.children().load(&bb)?;
        children = left_combine_children(ac.iter(), ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        target.push_detached(a, &ab)?;
        return Ok(());
    }
    target.push_new_unsplit(prefix, value, children, &NoStore)?;
    Ok(())
}

fn left_combine_children<'a, A, B, E, F>(
    a: NodeSeqIter<'a, A>,
    ab: A,
    b: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<NodeSeqBuilder, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, E> + Copy,
{
    let mut res = NodeSeqBuilder::new();
    let mut iter = OuterJoin::<A, B, E>::new(a, b);
    while let Some(x) = iter.next() {
        match x? {
            (Some(a), Some(b)) => {
                left_combine(&a, ab.clone(), &b, bb.clone(), f, &mut res)?;
            }
            (Some(a), None) => {
                res.push_detached(&a, &ab)?;
            }
            _ => {}
        }
    }
    Ok(res)
}

/// Inner combine two trees with a predicate f
fn left_combine_pred<A, B, E, F>(
    a: &TreeNode<A>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<bool, E> + Copy,
{
    let ap = a.prefix.load(&ab)?;
    let bp = b.prefix.load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        match (a.value.value_opt(), b.value.value_opt()) {
            (Some(av), Some(bv)) => {
                if f(av, bv)? {
                    return Ok(true);
                }
            }
            (Some(_), None) => return Ok(true),
            _ => {}
        };
        let ac = a.children.load(&ab)?;
        let bc = b.children.load(&bb)?;
        left_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == ap.len() {
        if a.value.is_some() {
            return Ok(true);
        };
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        let ac = a.children.load(&ab)?;
        left_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == bp.len() {
        let mut ac = NodeSeqBuilder::<A>::new();
        ac.push_shortened(a, &ab, n)?;
        let bc = b.children.load(&bb)?;
        left_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else {
        Ok(true)
    }
}

fn left_combine_children_pred<'a, A, B, E, F>(
    a: NodeSeqIter<'a, A>,
    ab: A,
    b: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<bool, E> + Copy,
{
    let mut iter = OuterJoin::<A, B, E>::new(a, b);
    while let Some(x) = iter.next() {
        match x? {
            (Some(a), Some(b)) => {
                if left_combine_pred(&a, ab.clone(), &b, bb.clone(), f)? {
                    return Ok(true);
                }
            }
            (Some(_), None) => return Ok(true),
            _ => {}
        }
    }
    Ok(false)
}

fn left_combine_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    let ap = a.peek().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        // move prefix and value
        let a = a.move_prefix();
        let a = if let (Some(av), Some(bv)) = (a.peek().value_opt(), b.value.value_opt()) {
            let r = f(av, bv)?;
            a.push_value_opt(r)
        } else {
            a.move_value()
        };
        let bc = b.children().load(&bb)?;
        left_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        let a = a.move_prefix().move_value();
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        left_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::new();
        let cursor = child.cursor();
        // store the last part of the prefix
        let cursor = cursor.push_prefix(&ap[n..]);
        // store the first part of the prefix
        let a = a.move_prefix_shortened(n, &ab)?;
        // store value from a in child, if it exists
        let cursor = cursor.push_value_raw(a.peek());
        // store the value from b, if it exists
        let a = a.push_value_none();
        // take the children
        let _ = cursor.push_children_raw(a.peek());
        let a = a.set_new_arc(Arc::new(child));
        let bc = b.children().load(&bb)?;
        left_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else {
        // the two nodes are disjoint
        a.move_prefix().move_value().move_children();
    }
    Ok(())
}

fn left_combine_children_with<'a, A, B, F>(
    a: InPlaceBuilderRef<'a, A, AtChildren>,
    ab: A,
    bc: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<InPlaceBuilderRef<'a, A, AtPrefix>, A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<OwnedBlob>, A::Error> + Copy,
{
    if bc.is_empty() {
        Ok(a.move_children())
    } else if a.peek().is_empty() {
        Ok(a.push_children_empty())
    } else {
        a.mutate(ab.clone(), move |ac| {
            let mut c = Combiner::<A, B>::new(ac, bc);
            while let Some(ordering) = c.cmp()? {
                match ordering {
                    Ordering::Less => {
                        c.a.move_one();
                    }
                    Ordering::Equal => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        let start = c.a.cursor().mark();
                        left_combine_with(c.a.cursor(), ab.clone(), &b, bb.clone(), f)?;
                        c.a.cursor().rewind(start);
                        // only move if the child is non-empty
                        c.a.move_non_empty();
                    }
                    Ordering::Greater => {
                        // skip one from b
                        let _ = c.b.next();
                    }
                }
            }
            Ok(())
        })
    }
}

fn retain_prefix_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<B>) -> Result<bool, B::Error> + Copy,
{
    let ap = a.peek().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        let a = a.move_prefix();
        let keep = b.value().is_none() || !f(&b.value().value_opt().unwrap())?;
        if keep {
            let a = a.push_value_none();
            let bc = b.children().load(&bb)?;
            retain_children_prefix_with(a, ab, bc.iter(), bb, f)?;
        } else {
            let _ = a.move_value().move_children();
        }
    } else if n == ap.len() {
        let a = a.move_prefix().push_value_none();
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        retain_children_prefix_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        if b.value().is_none() {
            // split a at n
            let mut child = NodeSeqBuilder::<A>::new();
            let cursor = child.cursor();
            // store the last part of the prefix
            let cursor = cursor.push_prefix(&ap[n..]);
            // store the first part of the prefix
            let a = a.move_prefix_shortened(n, &ab)?;
            // store value from a in child, if it exists
            let cursor = cursor.push_value_raw(a.peek());
            // store the value from b, if it exists
            let a = a.push_value_none();
            // take the children
            let _ = cursor.push_children_raw(a.peek());
            let a = a.set_new_arc(Arc::new(child));
            let bc = b.children.load(&bb)?;
            retain_children_prefix_with(a, ab, bc.iter(), bb, f)?;
        } else if !f(&b.value().value_opt().unwrap())? {
            a.push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        } else {
            a.move_prefix().move_value().move_children();
        }
    } else {
        // the two nodes are disjoint, nothing to do
        a.push_prefix_empty()
            .push_value_none()
            .push_children_empty();
    }
    Ok(())
}

fn retain_children_prefix_with<'a, A, B, F>(
    a: InPlaceBuilderRef<'a, A, AtChildren>,
    ab: A,
    bc: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<InPlaceBuilderRef<'a, A, AtPrefix>, A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<B>) -> Result<bool, B::Error> + Copy,
{
    if bc.is_empty() {
        Ok(a.push_children_empty())
    } else if a.peek().is_empty() {
        Ok(a.push_children_empty())
    } else {
        a.mutate(ab.clone(), move |ac| {
            let mut c = Combiner::<A, B>::new(ac, bc);
            while let Some(ordering) = c.cmp()? {
                match ordering {
                    Ordering::Less => {
                        // drop one from a
                        c.a.drop_one();
                    }
                    Ordering::Equal => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        let start = c.a.cursor().mark();
                        retain_prefix_with(c.a.cursor(), ab.clone(), &b, bb.clone(), f)?;
                        c.a.cursor().rewind(start);
                        // only move if the child is non-empty
                        c.a.move_non_empty();
                    }
                    Ordering::Greater => {
                        // skip one from b
                        let _ = c.b.next();
                    }
                }
            }
            Ok(())
        })
    }
}

fn remove_prefix_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: A,
    b: &TreeNode<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<B>) -> Result<bool, B::Error> + Copy,
{
    let ap = a.peek().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        if b.value().is_some() && f(&b.value().value_opt().unwrap())? {
            let _ = a
                .push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        } else {
            let a = a.move_prefix().move_value();
            let bc = b.children().load(&bb)?;
            remove_children_prefix_with(a, ab, bc.iter(), bb, f)?;
        }
    } else if n == ap.len() {
        let a = a.move_prefix().move_value();
        let mut bc = NodeSeqBuilder::<B>::new();
        bc.push_shortened(b, &bb, n)?;
        remove_children_prefix_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        if b.value().is_some() && f(&b.value().value_opt().unwrap())? {
            a.push_prefix_empty()
                .push_value_none()
                .push_children_empty();
        } else {
            // split a at n
            let mut child = NodeSeqBuilder::<A>::new();
            let cursor = child.cursor();
            // store the last part of the prefix
            let cursor = cursor.push_prefix(&ap[n..]);
            // store the first part of the prefix
            let a = a.move_prefix_shortened(n, &ab)?;
            // store value from a in child, if it exists
            let cursor = cursor.push_value_raw(a.peek());
            // store the value from b, if it exists
            let a = a.push_value_none();
            // take the children
            let _ = cursor.push_children_raw(a.peek());
            let a = a.set_new_arc(Arc::new(child));
            let bc = b.children().load(&bb)?;
            remove_children_prefix_with(a, ab, bc.iter(), bb, f)?;
        }
    } else {
        // the two nodes are disjoint, nothing to do
        a.move_prefix().move_value().move_children();
    }
    Ok(())
}

fn remove_children_prefix_with<'a, A, B, F>(
    a: InPlaceBuilderRef<'a, A, AtChildren>,
    ab: A,
    bc: NodeSeqIter<'a, B>,
    bb: B,
    f: F,
) -> Result<InPlaceBuilderRef<'a, A, AtPrefix>, A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<B>) -> Result<bool, B::Error> + Copy,
{
    if bc.is_empty() {
        Ok(a.move_children())
    } else if a.peek().is_empty() {
        Ok(a.push_children_empty())
    } else {
        a.mutate(ab.clone(), move |ac| {
            let mut c = Combiner::<A, B>::new(ac, bc);
            while let Some(ordering) = c.cmp()? {
                match ordering {
                    Ordering::Less => {
                        c.a.move_one();
                    }
                    Ordering::Equal => {
                        // the .unwrap() is safe because cmp guarantees that there is a value
                        let b = c.b.next().unwrap();
                        let start = c.a.cursor().mark();
                        remove_prefix_with(c.a.cursor(), ab.clone(), &b, bb.clone(), f)?;
                        c.a.cursor().rewind(start);
                        // only move if the child is non-empty
                        c.a.move_non_empty();
                    }
                    Ordering::Greater => {
                        // skip one from b
                        let _ = c.b.next();
                    }
                }
            }
            Ok(())
        })
    }
}

enum FindResult<T> {
    // Found an exact match
    Found(T),
    // found a tree for which the path is a prefix, with n remaining chars in the prefix of T
    Prefix {
        // a tree of which the searched path is a prefix
        tree: T,
        // number of bytes in the prefix of tree that are matching the end of the searched for prefix
        matching: usize,
    },
    // did not find anything, T is the closest match, with n remaining (unmatched) in the prefix of T
    NotFound,
}

/// find a prefix in a tree. Will either return
/// - Found(tree) if we found the tree exactly,
/// - Prefix if we found a tree of which prefix is a prefix
/// - NotFound if there is no tree
fn find<S: BlobStore, T>(
    store: &S,
    owner: &OwnedBlob,
    tree: &TreeNode<S>,
    prefix: &[u8],
    f: impl Fn(&OwnedBlob, FindResult<&TreeNode<S>>) -> Result<T, S::Error>,
) -> Result<T, S::Error> {
    let tree_prefix = tree.prefix().load(store)?;
    let n = common_prefix(&tree_prefix, prefix);
    // remaining in tree prefix
    let rt = tree_prefix.len() - n;
    // remaining in prefix
    let rp = prefix.len() - n;
    let fr = if rp == 0 && rt == 0 {
        // direct hit
        FindResult::Found(tree)
    } else if rp == 0 {
        // tree is a subtree of prefix
        FindResult::Prefix { tree, matching: n }
    } else if rt == 0 {
        // prefix is a subtree of tree
        let c = prefix[n];
        let tree_children = tree.children().load_owned(store)?;
        if let Some(child) = tree_children.find(c) {
            return find(store, &tree_children.0, &child, &prefix[n..], f);
        } else {
            FindResult::NotFound
        }
    } else {
        // disjoint, but we still need to store how far we matched
        FindResult::NotFound
    };
    f(owner, fr)
}

fn scan_prefix<S: BlobStore + Clone>(
    store: S,
    owner: &OwnedBlob,
    tree: &TreeNode<S>,
    prefix: &[u8],
) -> Result<Iter<S>, S::Error> {
    let store1 = store.clone();
    find(&store, owner, tree, prefix, |_, r| {
        let store1 = store1.clone();
        Ok(match r {
            FindResult::Found(tree) => {
                let mut t = NodeSeqBuilder::new();
                t.cursor()
                    .push_prefix_empty()
                    .push_value_raw(tree.value())
                    .push_children_raw(tree.children());
                Iter::new(
                    OwnedNodeSeqIter::new(Arc::new(t).as_owned_blob()),
                    store1,
                    IterKey::new(prefix),
                )
            }
            FindResult::Prefix { tree, matching } => {
                let mut t = NodeSeqBuilder::new();
                t.cursor()
                    .push_prefix_raw(tree.prefix())
                    .push_value_raw(tree.value())
                    .push_children_raw(tree.children());
                Iter::new(
                    OwnedNodeSeqIter::new(Arc::new(t).as_owned_blob()),
                    store1,
                    IterKey::new(&prefix[..prefix.len() - matching]),
                )
            }
            FindResult::NotFound => Iter::empty(store1),
        })
    })
}

#[derive(Debug, Default)]
pub(crate) struct IterKey(Arc<Vec<u8>>);

impl IterKey {
    fn new(root: &[u8]) -> Self {
        Self(Arc::new(root.to_vec()))
    }

    fn as_owned_blob(&self) -> OwnedBlob {
        OwnedBlob::from_arc_vec(self.0.clone())
    }

    fn append(&mut self, data: &[u8]) {
        // for typical iterator use, a reference is not kept for a long time, so this will be very cheap
        //
        // in the case a reference is kept, this will make a copy.
        let elems = Arc::make_mut(&mut self.0);
        elems.extend_from_slice(data);
    }

    fn pop(&mut self, n: usize) {
        let elems = Arc::make_mut(&mut self.0);
        elems.truncate(elems.len().saturating_sub(n));
    }
}

pub struct Iter<S: BlobStore> {
    path: IterKey,
    stack: Vec<(usize, Option<OwnedBlob>, OwnedNodeSeqIter<S>)>,
    store: S,
}

impl<S: BlobStore> Iter<S> {
    fn empty(store: S) -> Self {
        Self {
            stack: Vec::new(),
            path: IterKey::default(),
            store,
        }
    }

    fn new(iter: OwnedNodeSeqIter<S>, store: S, prefix: IterKey) -> Self {
        Self {
            stack: vec![(0, None, iter)],
            path: prefix,
            store,
        }
    }

    fn top_value(&mut self) -> &mut Option<OwnedBlob> {
        &mut self.stack.last_mut().unwrap().1
    }

    fn top_prefix_len(&self) -> usize {
        self.stack.last().unwrap().0
    }

    fn next0(&mut self) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        while !self.stack.is_empty() {
            if let Some((value, node)) = self.stack.last_mut().unwrap().2.next_value_and_node() {
                let prefix = node.prefix.load(&self.store)?;
                let prefix_len = prefix.len();
                let children = node.children.load_owned(&self.store)?.owned_iter();
                self.path.append(prefix.as_ref());
                self.stack.push((prefix_len, value, children));
            } else {
                if let Some(value) = self.top_value().take() {
                    let value = TreeValueRefWrapper(value, PhantomData);
                    return Ok(Some((self.path.as_owned_blob(), value)));
                } else {
                    self.path.pop(self.top_prefix_len());
                    self.stack.pop();
                }
            }
        }
        Ok(None)
    }
}

impl<S: BlobStore> Iterator for Iter<S> {
    type Item = Result<(OwnedBlob, TreeValueRefWrapper<S>), S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next0() {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(cause) => {
                // ensure that the next call to next will terminate
                self.stack.clear();
                Some(Err(cause))
            }
        }
    }
}

struct GroupBy<S: BlobStore, F> {
    path: IterKey,
    stack: Vec<(usize, OwnedNodeSeqIter<S>)>,
    store: S,
    descend: F,
}

impl<S: BlobStore, F: Fn(&[u8], &TreeNode<S>) -> bool> GroupBy<S, F> {
    fn new(iter: OwnedNodeSeqIter<S>, store: S, prefix: IterKey, descend: F) -> Self {
        Self {
            stack: vec![(0, iter)],
            path: prefix,
            store,
            descend,
        }
    }

    fn top_prefix_len(&self) -> usize {
        self.stack.last().unwrap().0
    }

    fn next0(&mut self) -> Result<Option<NodeSeqBuilder<S>>, S::Error> {
        Ok(loop {
            if self.stack.is_empty() {
                break None;
            }
            if let Some(node) = self.stack.last_mut().unwrap().1.next_node() {
                // apply the prefix in any case!
                let prefix = node.prefix.load(&self.store)?;
                let prefix_len = prefix.len();
                self.path.append(prefix.as_ref());
                if (self.descend)(&self.path.0, &node) {
                    let children = node.children.load_owned(&self.store)?.owned_iter();
                    let res = if node.value().is_some() {
                        let mut t = NodeSeqBuilder::new();
                        t.cursor()
                            .push_prefix(self.path.0.as_ref())
                            .push_value_raw(node.value())
                            .push_children_empty();
                        Some(t)
                    } else {
                        None
                    };
                    self.stack.push((prefix_len, children));
                    if let Some(t) = res {
                        break Some(t);
                    }
                } else {
                    let mut res = NodeSeqBuilder::new();
                    res.cursor()
                        .push_prefix(self.path.0.as_ref())
                        .push_value_raw(node.value())
                        .push_children_raw(node.children());
                    // undo applying the prefix immediately, since we don't descend
                    self.path.pop(prefix_len);
                    break Some(res);
                }
            } else {
                // undo applying the prefix
                self.path.pop(self.top_prefix_len());
                self.stack.pop();
            }
        })
    }
}

impl<S: BlobStore, F: Fn(&[u8], &TreeNode<S>) -> bool> Iterator for GroupBy<S, F> {
    type Item = Result<NodeSeqBuilder<S>, S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next0() {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(cause) => {
                // ensure that the next call to next will terminate
                self.stack.clear();
                Some(Err(cause))
            }
        }
    }
}

pub struct Values<S: BlobStore> {
    stack: Vec<OwnedNodeSeqIter<S>>,
    store: S,
}

impl<S: BlobStore> Values<S> {
    fn new(iter: OwnedNodeSeqIter<S>, store: S) -> Self {
        Self {
            stack: vec![iter],
            store,
        }
    }

    fn next0(&mut self) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        while !self.stack.is_empty() {
            if let Some((value, node)) = self.stack.last_mut().unwrap().next_value_and_node() {
                let children = node.children.load_owned(&self.store)?.owned_iter();
                self.stack.push(children);
                if let Some(value) = value {
                    let value = TreeValueRefWrapper(value, PhantomData);
                    return Ok(Some(value));
                }
            } else {
                self.stack.pop();
            }
        }
        Ok(None)
    }
}

impl<S: BlobStore> Iterator for Values<S> {
    type Item = Result<TreeValueRefWrapper<S>, S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next0() {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(cause) => {
                // ensure that the next call to next will terminate
                self.stack.clear();
                Some(Err(cause))
            }
        }
    }
}

// impl<K: Into<OwnedTreePrefix>, V: Into<OwnedTreeValue>> FromIterator<(K, V)> for Tree {
//     fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
//         let mut tree = Tree::empty();
//         for (k, v) in iter.into_iter() {
//             tree.outer_combine_with(
//                 &Tree::single(k.into().as_ref(), v.into().as_ref()),
//                 |_, b| Some(b.to_owned()),
//             );
//         }
//         tree
//     }
// }

impl FromIterator<(Vec<u8>, Vec<u8>)> for Tree {
    fn from_iter<T: IntoIterator<Item = (Vec<u8>, Vec<u8>)>>(iter: T) -> Self {
        let mut tree = Tree::empty();
        for (k, v) in iter.into_iter() {
            tree.outer_combine_with(&Tree::single(k.as_ref(), v.as_ref()), |_, b| {
                Some(b.to_owned())
            });
        }
        tree
    }
}

#[cfg(test)]
mod tests {
    use obey::{binary_element_test, binary_property_test, TestSamples};
    use proptest::prelude::*;
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Instant,
    };

    use super::*;

    fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(b'0'..b'9', 0..9)
    }

    fn arb_value() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..9)
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
            // let r1 = a.outer_combine(&b, |_, b| Some(b.to_owned()));
            let mut r2 = a.clone();
            r2.outer_combine_with(&b, |a, b| Some(b.to_owned()));
            // prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
            // check left biased union
            // let r1 = a.outer_combine(&b, |a, _| Some(a.to_owned()));
            let mut r2 = a.clone();
            r2.outer_combine_with(&b, |a, _| Some(a.to_owned()));
            // prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
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
            r2.inner_combine_with(&b, |_, b| Some(b.to_owned()));
            prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));
            // left biased intersection
            let r1 = a.inner_combine(&b, |a, _| Some(a.to_owned()));
            let mut r2 = a.clone();
            r2.inner_combine_with(&b, |a, _| Some(a.to_owned()));
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
            r2.left_combine_with(&b, |a, _| Some(a.to_owned()));
            prop_assert_eq!(to_btree_map(&r1), to_btree_map(&r2));

            let r1 = a.left_combine(&b, |_, b| Some(b.to_owned()));
            let mut r2 = a.clone();
            r2.left_combine_with(&b, |a, b| Some(b.to_owned()));
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
            prop_assert!(binary_property_test(&a, &b, !is_not_subset, |a, b| !a.is_some() | b.is_some()))
        }

        #[test]
        fn scan_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let filtered = tree.scan_prefix(&prefix);
            for (k, v) in filtered {
                prop_assert!(k.as_ref().starts_with(&prefix));
                let t = reference.get(k.as_ref()).unwrap();
                prop_assert_eq!(v.as_ref(), t);
            }
        }

        #[test]
        fn filter_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let filtered = tree.filter_prefix(&prefix);
            for (k, v) in filtered.iter() {
                prop_assert!(k.as_ref().starts_with(&prefix));
                let t = reference.get(k.as_ref()).unwrap();
                prop_assert_eq!(v.as_ref(), t);
            }
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
        fn group_by_true(a in arb_tree_contents()) {
            let at = mk_owned_tree(&a);
            let trees: Vec<Tree> = at.group_by(|_, _| true).collect::<Vec<_>>();
            prop_assert_eq!(a.len(), trees.len());
            for ((k0, v0), tree) in a.iter().zip(trees) {
                let k0: &[u8] = &k0;
                let v0: &[u8] = &v0;
                let k1 = tree.node().prefix().load(&NoStore).unwrap().to_vec();
                let v1 = tree.node().value().value_opt().map(|x| x.load(&NoStore).unwrap().to_vec());
                prop_assert!(tree.node().children().is_empty());
                prop_assert_eq!(k0, k1);
                prop_assert_eq!(Some(v0.to_vec()), v1);
            }
        }

        #[test]
        fn group_by_fixed(a in arb_tree_contents(), n in 0usize..8) {
            let at = mk_owned_tree(&a);
            let trees: Vec<Tree> = at.group_by(|x, _| x.len() <= n).collect::<Vec<_>>();
            prop_assert!(trees.len() <= a.len());
            let mut leafs = BTreeMap::new();
            for tree in &trees {
                let prefix = tree.node().prefix().load(&NoStore).unwrap();
                if prefix.len() <= n {
                    prop_assert!(tree.node().children().is_empty());
                    prop_assert!(tree.node().value().is_some());
                    let value = tree.node().value().load(&NoStore).unwrap().unwrap();
                    let prev = leafs.insert(prefix.to_vec(), value.to_vec());
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
    }

    #[test]
    fn union_with1() {
        let a = btreemap! { vec![1] => vec![], vec![2] => vec![] };
        let b = btreemap! { vec![1, 2] => vec![], vec![2] => vec![] };
        let mut at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        at.outer_combine_with(&bt, |a, b| Some(b.to_owned()));
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
        at.outer_combine_with(&bt, |a, b| Some(b.to_owned()));
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
        at.outer_combine_with(&bt, |a, b| Some(b.to_owned()));
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
        at.inner_combine_with(&bt, |_, b| Some(b.to_owned()));
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
        at.inner_combine_with(&bt, |_, b| Some(b.to_owned()));
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
        at.left_combine_with(&bt, |a, b| Some(b.to_owned()));
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
        let a = btreemap! { vec![1, 2] => vec![] };
        let b = btreemap! { vec![1] => vec![] };
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

    #[test]
    fn union_smoke() -> anyhow::Result<()> {
        println!("disjoint");
        let a = Tree::single(b"a".as_ref(), b"1".as_ref());
        let b = Tree::single(b"b".as_ref(), b"2".as_ref());
        let mut r = a;
        r.outer_combine_with(&b, |_, b| Some(b.to_owned()));
        r.dump()?;

        println!("same prefix");
        let a = Tree::single(b"ab".as_ref(), b"1".as_ref());
        let b = Tree::single(b"ab".as_ref(), b"2".as_ref());
        let mut r = a;
        r.outer_combine_with(&b, |_, b| Some(b.to_owned()));
        r.dump()?;

        println!("a prefix of b");
        let a = Tree::single(b"a".as_ref(), b"1".as_ref());
        let b = Tree::single(b"ab".as_ref(), b"2".as_ref());
        let mut r = a;
        r.outer_combine_with(&b, |_, b| Some(b.to_owned()));
        r.dump()?;

        println!("b prefix of a");
        let a = Tree::single(b"ab".as_ref(), b"1".as_ref());
        let b = Tree::single(b"a".as_ref(), b"2".as_ref());
        let mut r = a;
        r.outer_combine_with(&b, |_, b| Some(b.to_owned()));
        r.dump()?;

        Ok(())
    }

    #[test]
    fn build_bench() {
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
            .collect::<BTreeMap<_, _>>();
        let elems1 = elems.clone();
        let elems2 = elems.clone();
        let t0 = Instant::now();
        println!("building tree");
        let tree: Tree = elems.into_iter().collect();
        println!("unattached tree {} s", t0.elapsed().as_secs_f64());

        let mut count = 0;
        let t0 = Instant::now();
        for (k, v) in tree.iter() {
            count += 1;
        }
        println!("iterated tree {} s, {}", t0.elapsed().as_secs_f64(), count);

        let mut count = 0;
        let t0 = Instant::now();
        for (k, v) in elems1 {
            count += 1;
        }
        println!(
            "iterated btreemap {} s, {}",
            t0.elapsed().as_secs_f64(),
            count
        );

        let mut count = 0;
        let t0 = Instant::now();
        for v in tree.values() {
            count += 1;
        }
        println!(
            "iterated tree values {} s, {}",
            t0.elapsed().as_secs_f64(),
            count
        );

        let mut count = 0;
        let t0 = Instant::now();
        for v in elems2.values() {
            count += 1;
        }
        println!(
            "iterated btreemap values {} s, {}",
            t0.elapsed().as_secs_f64(),
            count
        );

        let mut count = 0;
        let t0 = Instant::now();
        for k in elems2.keys() {
            if tree.get(k.as_ref()).is_some() {
                count += 1;
            }
        }
        println!("get all values {} s, {}", t0.elapsed().as_secs_f64(), count);

        let t0 = Instant::now();
        let mut n = 0;
        for key in elems2.keys() {
            if elems2.get(key).is_some() {
                n += 1;
            }
        }
        println!(
            "get all values reference {} items, {} s",
            n,
            t0.elapsed().as_secs_f32()
        );
        // println!("{}", MUTATE_COUNTER.load(std::sync::atomic::Ordering::SeqCst));
        // println!("{}", GROW_COUNTER.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn smoke() {
        let a = Tree::single(b"aaaa", b"b");
        let b = Tree::single(b"aa", b"d");
        let r = a.outer_combine(&b, |_, b| Some(b.to_owned()));
        println!("{:?}", r);
        println!("{:?}", r.node());

        let mut t = Tree::empty();
        for i in 0u64..100 {
            let txt = i.to_string();
            let b = txt.as_bytes();
            t = t.outer_combine(&Tree::single(b, b), |_, b| Some(b.to_owned()))
        }
        for (k, v) in t.iter() {
            println!("{:?} {:?}", k, v);
        }
        // println!("{:?}", t);
        // println!("{:?}", t.node.iter().next().unwrap().unwrap());
        // t.dump().unwrap();
        // println!("{}", std::mem::size_of::<Arc<String>>());
        // println!("{}", std::mem::size_of::<Option<Arc<String>>>());

        // for i in 0..1000 {
        //     let res = t.try_get(i.to_string().as_bytes()).unwrap();
        //     println!("{:?}", res);
        // }

        println!("---");

        let t0 = std::time::Instant::now();
        let mut t = Tree::empty();
        for i in 0u64..1000000 {
            let txt = i.to_string();
            let b = txt.as_bytes();
            t.outer_combine_with(&Tree::single(b, b), |_, b| Some(b.to_owned()))
        }
        println!("create {}", t0.elapsed().as_secs_f64());

        // for i in 0..10000 {
        //     let res = t.try_get(i.to_string().as_bytes()).unwrap();
        //     println!("{:?}", res);
        // }

        // for (k, v) in t.iter() {
        //     println!("{:?} {:?}", k, v);
        // }

        // println!("{:?}", t);
    }
}
