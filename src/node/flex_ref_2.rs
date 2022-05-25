use std::{borrow::BorrowMut, cmp::Ordering, marker::PhantomData, ops::Deref, sync::Arc};

use crate::{
    store::{unwrap_safe, Blob, BlobStore2 as BlobStore, NoError, NoStore},
    Hex,
};
use std::fmt::Debug;

#[repr(C)]
struct FlexRef<T>(u8, PhantomData<T>, [u8]);

impl<T> Debug for FlexRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match tpe(self.0) {
            Type::None => write!(f, "FlexRef::None"),
            Type::Id => write!(f, "FlexRef::Id"),
            Type::Inline => write!(
                f,
                "FlexRef::Inline({})",
                Hex::new(self.inline_as_ref().unwrap())
            ),
            Type::Arc => write!(f, "FlexRef::Arc"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Type {
    None,
    Inline,
    Id,
    Arc,
}

#[repr(transparent)]
struct TreePrefixRef<S = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreePrefixRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreePrefix({:?})", &self.1)
    }
}

impl<S: BlobStore> TreePrefixRef<S> {
    fn first_opt(&self) -> Option<u8> {
        self.1.first_u8_opt()
    }

    fn bytes_len(&self) -> usize {
        self.1.bytes_len()
    }

    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn load(&self, store: &S) -> Result<TreePrefix, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            TreePrefix::from_slice(x)
        } else if let Some(x) = self.1.arc_as_clone() {
            TreePrefix::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_slice() {
            TreePrefix::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }

    fn read(value: &[u8]) -> Result<&Self, S::Error> {
        Ok(Self::new(FlexRef::read(value)?))
    }

    fn read_one<'a>(value: &'a [u8]) -> Result<(&'a Self, &'a [u8]), S::Error> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Ok((Self::new(f), rest))
    }

    fn empty() -> &'static Self {
        Self::new(FlexRef::<Vec<u8>>::empty())
    }

    fn manual_drop(&self) {
        self.1.manual_drop();
    }

    fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

impl AsRef<[u8]> for TreePrefixRef {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}

enum OwnedSlice {
    Arc(Arc<Vec<u8>>),
    Inline(Vec<u8>),
    Blob(Blob),
}

impl From<&[u8]> for OwnedSlice {
    fn from(v: &[u8]) -> Self {
        Self::Inline(v.to_vec())
    }
}

impl From<Vec<u8>> for OwnedSlice {
    fn from(v: Vec<u8>) -> Self {
        Self::Inline(v)
    }
}

impl From<Arc<Vec<u8>>> for OwnedSlice {
    fn from(v: Arc<Vec<u8>>) -> Self {
        Self::Arc(v)
    }
}

impl OwnedSlice {
    fn empty() -> Self {
        Self::Inline(Vec::new())
    }

    fn from_blob(blob: Blob) -> Self {
        Self::Blob(blob)
    }

    fn from_arc_vec(arc: Arc<Vec<u8>>) -> Self {
        Self::Arc(arc)
    }

    fn from_slice(v: &[u8]) -> Self {
        Self::Inline(v.to_vec())
    }
}

impl AsRef<[u8]> for OwnedSlice {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Arc(x) => x.as_ref().as_ref(),
            Self::Inline(x) => x.as_ref(),
            Self::Blob(blob) => blob.as_ref(),
        }
    }
}

impl Deref for OwnedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

struct TreeValue(OwnedSlice);

impl Debug for TreeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TreeValue")
            .field(&Hex::new(self.as_ref()))
            .finish()
    }
}

impl From<&[u8]> for TreeValue {
    fn from(v: &[u8]) -> Self {
        Self(v.into())
    }
}

impl From<Vec<u8>> for TreeValue {
    fn from(v: Vec<u8>) -> Self {
        Self(v.into())
    }
}

impl From<Arc<Vec<u8>>> for TreeValue {
    fn from(v: Arc<Vec<u8>>) -> Self {
        Self(v.into())
    }
}

impl TreeValue {
    fn empty() -> Self {
        Self(OwnedSlice::empty())
    }

    fn from_blob(blob: Blob) -> Self {
        Self(OwnedSlice::from_blob(blob))
    }

    fn from_arc_vec(arc: Arc<Vec<u8>>) -> Self {
        Self(arc.into())
    }

    fn from_slice(v: &[u8]) -> Self {
        Self(v.into())
    }
}

impl AsRef<[u8]> for TreeValue {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for TreeValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

struct TreePrefix(OwnedSlice);

impl From<&[u8]> for TreePrefix {
    fn from(v: &[u8]) -> Self {
        Self(v.into())
    }
}

impl From<Vec<u8>> for TreePrefix {
    fn from(v: Vec<u8>) -> Self {
        Self(v.into())
    }
}

impl From<Arc<Vec<u8>>> for TreePrefix {
    fn from(v: Arc<Vec<u8>>) -> Self {
        Self(v.into())
    }
}

impl TreePrefix {
    fn append<S: BlobStore>(&mut self, that: &TreePrefixRef<S>, store: &S) -> Result<(), S::Error> {
        todo!()
    }

    fn empty() -> Self {
        Self(OwnedSlice::empty())
    }

    fn from_blob(blob: Blob) -> Self {
        Self(OwnedSlice::from_blob(blob))
    }

    fn from_arc_vec(arc: Arc<Vec<u8>>) -> Self {
        Self(arc.into())
    }

    fn from_slice(v: &[u8]) -> Self {
        Self(v.into())
    }
}

impl AsRef<[u8]> for TreePrefix {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for TreePrefix {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[repr(transparent)]
struct TreeValueRef<S: BlobStore = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

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

    fn load(&self, store: &S) -> Result<TreeValue, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            TreeValue::from_slice(x)
        } else if let Some(x) = self.1.arc_as_clone() {
            TreeValue::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_slice() {
            TreeValue::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }
}

impl TreeValueRef {
    fn to_owned(&self) -> TreeValue {
        unwrap_safe(self.load(&NoStore))
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

    fn bytes_len(&self) -> usize {
        self.1.bytes_len()
    }

    fn value_opt(&self) -> Option<&TreeValueRef<S>> {
        if self.is_none() {
            None
        } else {
            Some(TreeValueRef::new(&self.1))
        }
    }

    fn none() -> &'static Self {
        Self::new(FlexRef::<Vec<u8>>::none())
    }

    fn is_none(&self) -> bool {
        self.1.is_none()
    }

    fn is_some(&self) -> bool {
        self.1.is_some()
    }

    fn load(&self, store: &S) -> Result<Option<TreeValue>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            Some(TreeValue::from_slice(x))
        } else if let Some(x) = self.1.arc_as_clone() {
            Some(TreeValue::from_arc_vec(x))
        } else if let Some(id) = self.1.id_as_slice() {
            Some(TreeValue::from_blob(store.read(id)?))
        } else {
            None
        })
    }

    fn read_one<'a>(value: &'a [u8]) -> Result<(&'a Self, &'a [u8]), S::Error> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Ok((Self::new(f), rest))
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
            Type::Inline => write!(f, "TreeChildren::Empty"),
            Type::Arc => write!(f, "TreeChildren::Arc({:?})", self.1.arc_as_clone().unwrap()),
            Type::Id => write!(f, "TreeChildren::Id({:?})", self.1.id_as_slice().unwrap()),
            Type::None => write!(f, "TreeChildren invalid"),
        }
    }
}

impl<S: BlobStore> TreeChildrenRef<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn bytes_len(&self) -> usize {
        self.1.bytes_len()
    }

    fn read_one<'a>(value: &'a [u8]) -> Result<(&'a Self, &'a [u8]), S::Error> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Ok((Self::new(f), rest))
    }

    fn load(&self, store: &S) -> Result<NodeSeq<S>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            assert!(x.is_empty());
            NodeSeq::empty()
        } else if let Some(x) = self.1.arc_as_clone() {
            NodeSeq::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_slice() {
            NodeSeq::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }

    fn empty() -> &'static Self {
        Self::new(FlexRef::empty())
    }

    fn is_empty(&self) -> bool {
        self.1.is_empty()
    }

    fn manual_drop(&self) {
        self.1.manual_drop();
    }

    fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

enum NodeSeq<S: BlobStore> {
    Arc(Arc<NodeSeqBuilder<S>>),
    Empty,
    Blob(Blob),
}

impl<S: BlobStore> NodeSeq<S> {
    fn empty() -> Self {
        Self::Empty
    }

    fn from_arc_vec(value: Arc<NodeSeqBuilder<S>>) -> Self {
        Self::Arc(value)
    }

    fn from_blob(value: Blob) -> Self {
        Self::Blob(value)
    }
}

impl<S: BlobStore> AsRef<NodeSeqRef<S>> for NodeSeq<S> {
    fn as_ref(&self) -> &NodeSeqRef<S> {
        match self {
            Self::Empty => NodeSeqRef::new(&[]),
            Self::Arc(value) => value.as_ref(),
            Self::Blob(blob) => NodeSeqRef::new(blob.as_ref()),
        }
    }
}

impl<S: BlobStore> Deref for NodeSeq<S> {
    type Target = NodeSeqRef<S>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[repr(transparent)]
struct TreeChildrenIterator<'a, S>(&'a [u8], PhantomData<S>);

impl<'a, S: BlobStore> TreeChildrenIterator<'a, S> {
    fn peek(&self) -> Option<Result<Option<u8>, S::Error>> {
        if self.0.is_empty() {
            None
        } else {
            Some(TreePrefixRef::<S>::read(&self.0).map(|x| x.first_opt()))
        }
    }
}

impl<'a, S: BlobStore> Iterator for TreeChildrenIterator<'a, S> {
    type Item = Result<TreeNode<'a, S>, S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            Some(match TreeNode::read_one(&mut self.0) {
                Ok((res, rest)) => {
                    self.0 = rest;
                    Ok(res)
                }
                Err(e) => Err(e),
            })
        }
    }
}

struct TreeNode<'a, S: BlobStore> {
    data: &'a [u8],
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
    fn prefix(&self) -> &'a TreePrefixRef<S> {
        TreePrefixRef::new(FlexRef::new(&self.data))
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

    fn bytes(&self) -> &[u8] {
        &self.data
    }

    fn read(buffer: &'a [u8]) -> Result<Self, S::Error> {
        Ok(Self::read_one(buffer)?.0)
    }

    fn read_one(buffer: &'a [u8]) -> Result<(Self, &'a [u8]), S::Error> {
        let rest = buffer;
        let (prefix, rest) = TreePrefixRef::<S>::read_one(rest)?;
        let (value, rest) = TreeValueOptRef::<S>::read_one(rest)?;
        let (children, rest) = TreeChildrenRef::<S>::read_one(rest)?;
        let len = prefix.bytes_len() + value.bytes_len() + children.bytes_len();
        Ok((
            Self {
                data: &buffer[0..len],
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
        println!("{}TreeNode", spacer);
        println!("{}  prefix={:?}", spacer, self.prefix());
        println!("{}  value={:?}", spacer, self.value());
        println!("{}  children", spacer);
        for child in self.children().load(store)?.iter() {
            let child = child?;
            child.dump(indent + 4, store)?;
        }
        Ok(())
    }

    /// get the first value
    fn first_value(&self, store: &S) -> Result<Option<TreeValue>, S::Error> {
        Ok(if self.children().is_empty() {
            self.value().load(store)?
        } else {
            let children = self.children().load(store)?;
            children.iter().next().unwrap()?.first_value(store)?
        })
    }

    /// get the first entry
    fn first_entry(
        &self,
        store: &S,
        mut prefix: TreePrefix,
    ) -> Result<Option<(TreePrefix, TreeValue)>, S::Error> {
        prefix.append(&self.prefix(), store)?;
        Ok(if self.children().is_empty() {
            if let Some(value) = self.value().value_opt() {
                Some((prefix, value.load(store)?))
            } else {
                None
            }
        } else {
            let children = self.children().load(store)?;
            children
                .iter()
                .next()
                .unwrap()?
                .first_entry(store, prefix)?
        })
    }

    /// get the first value
    fn last_value(&self, store: &S) -> Result<Option<TreeValue>, S::Error> {
        Ok(if self.children().is_empty() {
            self.value().load(store)?
        } else {
            let children = self.children().load(store)?;
            children.iter().last().unwrap()?.first_value(store)?
        })
    }

    /// get the first entry
    fn last_entry(
        &self,
        store: &S,
        mut prefix: TreePrefix,
    ) -> Result<Option<(TreePrefix, TreeValue)>, S::Error> {
        prefix.append(&self.prefix(), store)?;
        Ok(if self.children().is_empty() {
            if let Some(value) = self.value().value_opt() {
                Some((prefix, value.load(store)?))
            } else {
                None
            }
        } else {
            let children = self.children().load(store)?;
            children
                .iter()
                .last()
                .unwrap()?
                .first_entry(store, prefix)?
        })
    }

    /// Return the subtree with the given prefix. Will return an empty tree in case there is no match.
    fn filter_prefix(&self, store: &S, prefix: &[u8]) -> Result<NodeSeqBuilder<S>, S::Error> {
        find(store, self, prefix, |x| {
            Ok(match x {
                FindResult::Found(res) => {
                    let mut b = NodeSeqBuilder::new();
                    todo!();
                    // let mut res = res.clone();
                    // res.prefix = TreePrefix::new(FlexRef::inline_or_owned_from_slice(prefix));
                    b
                }
                FindResult::Prefix { tree: res, rt } => {
                    let mut b = NodeSeqBuilder::new();
                    let rp = res.prefix().load(store)?;
                    todo!();
                    // let mut res = res.clone();
                    // res.prefix = TreePrefix::join(prefix, &rp[rp.len() - rt..]);
                    b
                }
                FindResult::NotFound { .. } => NodeSeqBuilder::empty_tree(),
            })
        })
    }
}

impl FlexRef<Vec<u8>> {
    fn first_u8_opt(&self) -> Option<u8> {
        match self.tpe() {
            Type::None => None,
            Type::Inline => self.2.get(0).cloned(),
            Type::Arc => self.with_arc(|x| x.as_ref().get(0).cloned()).unwrap(),
            Type::Id => todo!("pack first byte into id"),
        }
    }

    fn slice(data: &[u8], target: &mut Vec<u8>) {
        if data.len() < 128 {
            target.push_flexref_inline(data);
        } else {
            target.push_flexref_arc(Arc::new(data.to_vec()));
        }
    }
}

trait FlexRefExt {
    fn take(&mut self) -> Vec<u8>;

    fn push_flexref_none(&mut self);

    fn push_flexref_arc<T>(&mut self, arc: Arc<T>);

    fn push_flexref_inline(&mut self, data: &[u8]);

    fn push_flexref_arc_or_inline(&mut self, data: impl AsRef<[u8]>, max_inline: usize);
}

impl FlexRefExt for Vec<u8> {
    fn take(&mut self) -> Vec<u8> {
        let mut t = Vec::new();
        std::mem::swap(&mut t, self);
        t
    }

    fn push_flexref_arc<T>(&mut self, arc: Arc<T>) {
        self.push(0x01);
        let data: usize = unsafe { std::mem::transmute(arc) };
        let data: u64 = data as u64;
        self.extend_from_slice(&data.to_be_bytes());
    }

    fn push_flexref_inline(&mut self, data: &[u8]) {
        assert!(data.len() < 128);
        let l = (data.len() as u8) | 0x80;
        self.push(l);
        self.extend_from_slice(data);
    }

    fn push_flexref_arc_or_inline(&mut self, data: impl AsRef<[u8]>, max_inline: usize) {
        assert!(max_inline < 128);
        let data = data.as_ref();
        if data.len() <= max_inline {
            self.push_flexref_inline(data.as_ref())
        } else {
            self.push_flexref_arc(Arc::new(data.to_vec()))
        }
    }

    fn push_flexref_none(&mut self) {
        self.push(0u8);
    }
}

impl<T> FlexRef<T> {
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

    fn arc(data: Arc<T>, target: &mut Vec<u8>) {}

    fn new(value: &[u8]) -> &Self {
        unsafe { std::mem::transmute(value) }
    }

    fn none() -> &'static Self {
        Self::read_one(&[0]).unwrap().0
    }

    fn empty() -> &'static Self {
        Self::read_one(&[0x80]).unwrap().0
    }

    fn is_empty(&self) -> bool {
        self.0 == 0x80
    }

    fn read_one(value: &[u8]) -> anyhow::Result<(&Self, &[u8])> {
        anyhow::ensure!(value.len() > 0);
        let len = len(value[0]);
        anyhow::ensure!(len + 1 <= value.len());
        Ok((Self::new(value), &value[len + 1..]))
    }

    fn read(value: &[u8]) -> anyhow::Result<&Self> {
        Ok(Self::read_one(value)?.0)
    }

    fn tpe(&self) -> Type {
        tpe(self.0)
    }

    fn bytes_len(&self) -> usize {
        len(self.0) + 1
    }

    fn inline_as_ref(&self) -> Option<&[u8]> {
        if self.tpe() == Type::Inline {
            let len = len(self.0);
            Some(&self.2[0..len])
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

    fn with_arc<U>(&self, f: impl Fn(&Arc<T>) -> U) -> Option<U> {
        if self.tpe() == Type::Arc {
            let value = u64::from_be_bytes(self.2[0..8].try_into().unwrap());
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
            Some(f(&self.2))
        } else {
            None
        }
    }

    fn with_id<U>(&self, f: impl Fn(u64) -> U) -> Option<U> {
        if self.tpe() == Type::Id {
            let id = u64::from_be_bytes(self.2[0..8].try_into().unwrap());
            Some(f(id))
        } else {
            None
        }
    }

    fn is_none(&self) -> bool {
        self.tpe() == Type::None
    }

    fn is_some(&self) -> bool {
        !self.is_some()
    }

    fn copy_to(&self, target: &mut Vec<u8>) {
        self.with_arc(|arc| {
            std::mem::forget(arc.clone());
        });
        self.move_to(target)
    }

    fn move_to(&self, target: &mut Vec<u8>) {
        let len = len(self.0);
        target.push(self.0);
        target.extend_from_slice(&self.2[..len]);
    }
}

fn len(value: u8) -> usize {
    match tpe(value) {
        Type::None => 0,
        Type::Id => 8,
        Type::Arc => 8,
        Type::Inline => ((value & 0x7f) as usize),
    }
}

fn tpe(value: u8) -> Type {
    if value == 0 {
        Type::None
    } else if value & 0x80 != 0 {
        Type::Inline
    } else if value & 0x40 != 0 {
        Type::Id
    } else {
        Type::Arc
    }
}

#[repr(transparent)]
struct NodeSeqRef<S: BlobStore>(PhantomData<S>, [u8]);

impl<S: BlobStore> NodeSeqRef<S> {
    fn new(value: &[u8]) -> &Self {
        unsafe { std::mem::transmute(value) }
    }

    fn iter(&self) -> TreeChildrenIterator<'_, S> {
        TreeChildrenIterator(&self.1, PhantomData)
    }

    fn find(&self, first: u8) -> Result<Option<TreeNode<'_, S>>, S::Error> {
        // todo: optimize
        for leaf in self.iter() {
            let leaf = leaf?;
            let first_opt = leaf.prefix().first_opt();
            if first_opt == Some(first) {
                // found it
                return Ok(Some(leaf));
            } else if first_opt > Some(first) {
                // not going to come anymore
                return Ok(None);
            }
        }
        Ok(None)
    }
}

struct InPlaceFlexRefSeqBuilder {
    // place for the data. [..t1] and [s1..] contain valid sequences of flexrefs.
    // The rest is to be considered uninitialized
    vec: Vec<u8>,
    // end of the result
    t1: usize,
    // start of the source
    s0: usize,
}

impl InPlaceFlexRefSeqBuilder {
    /// Create a new builder by taking over the data from a vec
    pub fn new(vec: Vec<u8>) -> Self {
        Self { vec, t1: 0, s0: 0 }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.vec
    }

    fn drop(&mut self, n: usize) {
        debug_assert!(n <= self.source_slice().len());
        self.t1 += n;
    }

    fn mv(&mut self, n: usize) {
        debug_assert!(n <= self.source_slice().len());
        if self.gap() == 0 {
            self.t1 += n;
            self.s0 += n;
        } else {
            self.vec.copy_within(self.t1..self.t1 + n, self.s0);
            self.t1 += n;
            self.s0 += n;
        }
    }

    fn gap(&self) -> usize {
        self.s0 - self.t1
    }

    fn reserve(&mut self, n: usize) {
        let gap = self.gap();
        if gap < n {
            let missing = n - gap;
            self.vec
                .splice(self.s0..self.s0, std::iter::repeat(0).take(missing));
            self.s0 += missing;
        }
    }

    pub fn push_id(&mut self, id: &[u8]) {
        self.reserve(1);
        self.push(&[0]);
    }

    pub fn push_none(&mut self) {
        self.reserve(1);
        self.push(&[0]);
    }

    pub fn push_arc<T>(&mut self, arc: Arc<T>) {
        let data: usize = unsafe { std::mem::transmute(arc) };
        let data: u64 = data as u64;
        self.reserve(9);
        self.push(&[0x01]);
        self.push(&data.to_be_bytes());
    }

    pub fn push_arc_or_inline(&mut self, data: impl AsRef<[u8]>, max_inline: usize) {
        assert!(max_inline < 128);
        let data = data.as_ref();
        if data.len() <= max_inline {
            self.push_inline(data.as_ref())
        } else {
            self.push_arc(Arc::new(data.to_vec()))
        }
    }

    fn push_inline(&mut self, data: &[u8]) {
        assert!(data.len() < 128);
        let l = (data.len() as u8) | 0x80;
        self.reserve(data.len() + 1);
        self.push(&[l]);
        self.push(data);
    }

    fn push(&mut self, data: &[u8]) {
        let len = data.len();
        self.reserve(len);
        self.vec[self.t1..self.t1 + len].copy_from_slice(data);
    }

    fn target_slice(&self) -> &[u8] {
        &self.vec[..self.t1]
    }

    fn source_slice(&self) -> &[u8] {
        &self.vec[self.s0..]
    }
}

struct FlexRefIter<'a>(&'a [u8]);

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

struct AtPrefix;
struct AtValue;
struct AtChildren;

trait IterPosition {}
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

#[repr(transparent)]
struct InPlaceBuilderRef<'a, S: BlobStore, P: IterPosition>(
    &'a mut InPlaceFlexRefSeqBuilder,
    PhantomData<(S, P)>,
);

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtPrefix> {
    pub fn peek(&self) -> Result<&TreePrefixRef<S>, S::Error> {
        Ok(TreePrefixRef::new(FlexRef::new(self.0.source_slice())))
    }

    pub fn move_prefix(self) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        let len = self.peek()?.bytes_len();
        self.0.mv(len);
        self.next()
    }

    pub fn set_prefix(
        mut self,
        prefix: &[u8],
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        self.drop_current()?;
        todo!();
        // self.0.push(prefix);
        self.next()
    }

    pub fn insert_converted<S2: BlobStore>(
        mut self,
        prefix: &TreePrefixRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        todo!()
    }

    fn drop_current(&mut self) -> Result<(), S::Error> {
        let peek = self.peek()?;
        let len = peek.bytes_len();
        peek.manual_drop();
        self.0.drop(len);
        Ok(())
    }

    fn next(self) -> Result<InPlaceBuilderRef<'a, S, AtValue>, S::Error> {
        Ok(InPlaceBuilderRef(self.0, PhantomData))
    }
}

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtValue> {
    fn done(self) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        Ok(InPlaceBuilderRef(self.0, PhantomData))
    }

    fn drop_current(&mut self) {
        let len = self.peek().bytes_len();
        self.peek().manual_drop();
        self.0.drop(len);
    }

    pub fn peek(&self) -> &TreeValueOptRef<S> {
        TreeValueOptRef::new(FlexRef::new(self.0.source_slice()))
    }

    pub fn move_value(self) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        let len = self.peek().bytes_len();
        self.0.mv(len);
        self.done()
    }

    pub fn push_value_none(mut self) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        self.0.push_none();
        self.done()
    }

    pub fn push_value_opt(
        mut self,
        value: Option<TreeValue>,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        if let Some(value) = value {
            todo!()
        } else {
            self.push_value_none()
        }
    }

    pub fn push_value_ref(
        mut self,
        value: &TreeValueRef<S>,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        todo!()
    }

    pub fn insert_converted<S2: BlobStore>(
        mut self,
        value: &TreeValueRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        todo!()
    }

    pub fn insert_opt_converted<S2: BlobStore>(
        mut self,
        value: &TreeValueOptRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtChildren>, S::Error> {
        self.drop_current();
        todo!()
    }
}

impl<'a, S: BlobStore> InPlaceBuilderRef<'a, S, AtChildren> {
    /// peek the child at the current position as a TreeChildrenRef
    ///
    /// this returns an error in case the item extends behind the end of the buffer
    pub fn peek(&self) -> Result<&TreeChildrenRef<S>, S::Error> {
        Ok(TreeChildrenRef::new(FlexRef::new(self.0.source_slice())))
    }

    pub fn take_arc(&self, store: &S) -> Result<Arc<NodeSeqBuilder<S>>, S::Error> {
        // replace current value with "no children" paceholder
        todo!()
    }

    fn drop_current(&mut self) -> Result<(), S::Error> {
        let peek = self.peek()?;
        let len = peek.bytes_len();
        peek.manual_drop();
        self.0.drop(len);
        Ok(())
    }

    pub fn set_arc(
        mut self,
        value: Arc<NodeSeqBuilder<S>>,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        self.drop_current()?;
        todo!();
        self.done()
    }

    fn done(self) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        Ok(InPlaceBuilderRef(self.0, PhantomData))
    }

    pub fn move_children(self) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        self.0.mv(self.peek()?.bytes_len());
        self.done()
    }

    pub fn insert_converted<S2: BlobStore>(
        mut self,
        value: &TreeChildrenRef<S2>,
        store: &S2,
    ) -> Result<InPlaceBuilderRef<'a, S, AtPrefix>, S::Error> {
        todo!();
        self.done()
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
        Self {
            inner: InPlaceFlexRefSeqBuilder::new(from.0.take()),
            p: PhantomData,
        }
    }

    /// consumes an InPlaceNodeSeqBuilder by storing the result in a NodeSeqBuilder
    fn into_inner(mut self) -> NodeSeqBuilder<S> {
        let mut t = Vec::new();
        std::mem::swap(&mut self.inner.vec, &mut t);
        self.inner.t1 = 0;
        self.inner.s0 = 0;
        NodeSeqBuilder(t, PhantomData)
    }

    /// A cursor, assuming we are currently at the start of a triple
    fn cursor(&mut self) -> InPlaceBuilderRef<'_, S, AtPrefix> {
        InPlaceBuilderRef(&mut self.inner, PhantomData)
    }

    /// Peek element of the source
    fn peek(&self) -> Option<Result<Option<u8>, S::Error>> {
        if self.inner.source_slice().is_empty() {
            None
        } else {
            Some(Ok(TreePrefixRef::<S>::new(FlexRef::new(
                self.inner.source_slice(),
            ))
            .first_opt()))
        }
    }

    /// move one triple from the source to the target
    fn move_one(&mut self) -> Result<(), S::Error> {
        self.cursor().move_prefix()?.move_value()?.move_children()?;
        Ok(())
    }

    fn insert_converted<S2: BlobStore>(
        &mut self,
        node: TreeNode<S2>,
        store: &S2,
    ) -> Result<(), S::Error> {
        self.cursor()
            .insert_converted(node.prefix(), store)?
            .insert_opt_converted(node.value(), store)?
            .insert_converted(node.children(), store)?;
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
    fn new() -> Self {
        Self(Vec::new(), PhantomData)
    }

    fn into_inner(mut self) -> Vec<u8> {
        let mut r = Vec::new();
        std::mem::swap(&mut self.0, &mut r);
        drop(self);
        r
    }

    fn push_prefix(&mut self, prefix: TreePrefix) {
        self.0.push_flexref_arc_or_inline(prefix, 32);
    }

    fn push_value(&mut self, value: Option<TreeValue>) {
        if let Some(value) = value {
            self.0.push_flexref_arc_or_inline(value, 32);
        } else {
            self.0.push_flexref_none()
        }
    }

    fn push_children(&mut self, value: NodeSeqBuilder<S>) {
        if !value.0.is_empty() {
            self.0.push_flexref_arc(Arc::new(value))
        } else {
            self.0.push_flexref_inline(&[]);
        }
    }

    fn empty_tree() -> Self {
        let mut res = Self::new();
        res.push_prefix(TreePrefix::empty());
        res.push_value(None);
        res.push_children(NodeSeqBuilder::new());
        res
    }

    fn from_node(node: TreeNode<'_, S>) -> Self {
        let mut res = Self(Vec::new(), PhantomData);
        node.prefix().1.copy_to(&mut res.0);
        node.value().1.copy_to(&mut res.0);
        node.children().1.copy_to(&mut res.0);
        res
    }

    fn push_new(
        &mut self,
        prefix: TreePrefix,
        value: Option<TreeValue>,
        children: NodeSeqBuilder<S>,
    ) {
        self.push_prefix(prefix);
        self.push_value(value);
        self.push_children(children);
    }

    fn push_detached<S2: BlobStore>(
        &mut self,
        node: TreeNode<'_, S2>,
        store: &S2,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut NodeSeqBuilder<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    fn push_new_unsplit(
        &mut self,
        prefix: TreePrefix,
        value: Option<TreeValue>,
        children: NodeSeqBuilder<S>,
        store: &S,
    ) -> Result<(), S::Error> {
        // todo
        Ok(self.push_new(prefix, value, children))
    }

    fn push(&mut self, node: TreeNode<'_, S>) {
        node.prefix().1.copy_to(&mut self.0);
        node.value().1.copy_to(&mut self.0);
        node.children().1.copy_to(&mut self.0);
    }

    fn push_shortened(
        &mut self,
        node: &TreeNode<'_, S>,
        store: &S,
        n: usize,
    ) -> Result<(), S::Error> {
        if n > 0 {
            let prefix = node.prefix().load(store)?;
            assert!(n < prefix.len());
            FlexRef::slice(&prefix[n..], &mut self.0);
        } else {
            node.prefix().1.copy_to(&mut self.0);
        }
        node.value().1.copy_to(&mut self.0);
        node.children().1.copy_to(&mut self.0);
        Ok(())
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
        let mut t = Vec::new();
        FlexRef::<Vec<u8>>::slice(key, &mut t);
        FlexRef::<Vec<u8>>::slice(value, &mut t);
        FlexRef::<Vec<u8>>::empty().copy_to(&mut t);
        Self(t, PhantomData)
    }
}

impl<S: BlobStore> AsRef<NodeSeqRef<S>> for NodeSeqBuilder<S> {
    fn as_ref(&self) -> &NodeSeqRef<S> {
        NodeSeqRef::new(self.0.as_ref())
    }
}

impl<S: BlobStore> Deref for NodeSeqBuilder<S> {
    type Target = NodeSeqRef<S>;

    fn deref(&self) -> &Self::Target {
        NodeSeqRef::new(self.0.as_ref())
    }
}

impl<S: BlobStore> Clone for NodeSeqBuilder<S> {
    fn clone(&self) -> Self {
        for elem in self.as_ref().iter() {
            if let Ok(elem) = elem {
                elem.manual_clone();
            }
        }
        Self(self.0.clone(), PhantomData)
    }
}

impl<S: BlobStore> Drop for NodeSeqBuilder<S> {
    fn drop(&mut self) {
        for elem in self.as_ref().iter() {
            if let Ok(elem) = elem {
                elem.manual_drop();
            }
        }
        self.0.truncate(0);
    }
}

#[derive(Debug)]
struct Tree<S: BlobStore = NoStore> {
    /// This contains exactly one node, even in the case of an empty tree
    node: NodeSeqBuilder<S>,
    /// The associated store
    store: S,
}

impl Tree {
    fn empty() -> Self {
        Self {
            node: NodeSeqBuilder::empty_tree(),
            store: NoStore,
        }
    }

    fn single(key: &[u8], value: &[u8]) -> Self {
        Self {
            node: NodeSeqBuilder::single(key, value),
            store: NoStore,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<TreeValue> {
        unwrap_safe(self.try_get(key))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        unwrap_safe(self.try_contains_key(key))
    }

    pub fn first_value(&self) -> Option<TreeValue> {
        unwrap_safe(self.try_first_value())
    }

    pub fn last_value(&self) -> Option<TreeValue> {
        unwrap_safe(self.try_last_value())
    }

    pub fn first_entry(&self, prefix: TreePrefix) -> Option<(TreePrefix, TreeValue)> {
        unwrap_safe(self.try_first_entry(prefix))
    }

    pub fn last_entry(&self, prefix: TreePrefix) -> Option<(TreePrefix, TreeValue)> {
        unwrap_safe(self.try_last_entry(prefix))
    }

    pub fn outer_combine(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValueRef, &TreeValueRef) -> Option<TreeValue> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_outer_combine::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }
}

impl<S: BlobStore + Clone> Tree<S> {
    fn node(&self) -> TreeNode<'_, S> {
        self.node.iter().next().unwrap().unwrap()
    }

    fn dump(&self) -> Result<(), S::Error> {
        self.node().dump(0, &self.store)
    }

    /// Get the value for a given key
    fn try_get(&self, key: &[u8]) -> Result<Option<TreeValue>, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(&self.store, &self.node(), key, |r| {
            Ok(if let FindResult::Found(tree) = r {
                tree.value().load(&self.store)?
            } else {
                None
            })
        })
    }

    /// True if key is contained in this set
    fn try_contains_key(&self, key: &[u8]) -> Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(&self.store, &self.node(), key, |r| {
            Ok(if let FindResult::Found(tree) = r {
                tree.value().is_some()
            } else {
                false
            })
        })
    }

    pub fn try_outer_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValueRef<S>, &TreeValueRef<S2>) -> Result<Option<TreeValue>, E> + Copy,
    {
        let mut nodes = NodeSeqBuilder::new();
        outer_combine(
            &self.node.iter().next().unwrap()?,
            &self.store,
            &that.node.iter().next().unwrap()?,
            &that.store,
            f,
            &mut nodes,
        )?;
        Ok(Tree {
            node: nodes,
            store: NoStore,
        })
    }

    pub fn try_first_value(&self) -> Result<Option<TreeValue>, S::Error> {
        self.node().first_value(&self.store)
    }

    pub fn try_last_value(&self) -> Result<Option<TreeValue>, S::Error> {
        self.node().last_value(&self.store)
    }

    pub fn try_first_entry(
        &self,
        prefix: TreePrefix,
    ) -> Result<Option<(TreePrefix, TreeValue)>, S::Error> {
        self.node().first_entry(&self.store, prefix)
    }

    pub fn try_last_entry(
        &self,
        prefix: TreePrefix,
    ) -> Result<Option<(TreePrefix, TreeValue)>, S::Error> {
        self.node().last_entry(&self.store, prefix)
    }

    pub fn try_filter_prefix<'a>(&'a self, prefix: &[u8]) -> Result<Tree<S>, S::Error> {
        self.node()
            .filter_prefix(&self.store, prefix)
            .map(|node| Tree {
                node,
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
    ab: &A,
    b: &TreeNode<B>,
    bb: &B,
    f: F,
    target: &mut NodeSeqBuilder,
) -> Result<(), E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<TreeValue>, E> + Copy,
{
    let ap = a.prefix().load(ab)?;
    let bp = b.prefix().load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value().load(ab);
    let bv = || b.value().load(bb);
    let prefix = TreePrefix::from_slice(&ap[..n]);
    let mut children: NodeSeqBuilder = NodeSeqBuilder::new();
    let value: Option<TreeValue>;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = match (a.value().value_opt(), b.value().value_opt()) {
            (Some(a), Some(b)) => f(a, b)?,
            (Some(a), None) => Some(a.load(ab)?),
            (None, Some(b)) => Some(b.load(bb)?),
            (None, None) => None,
        };
        let ac = a.children().load(ab)?;
        let bc = b.children().load(bb)?;
        children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = av()?;
        let ac = a.children().load(ab)?;
        let bc = NodeSeqBuilder::shortened(b, bb, n)?;
        children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = bv()?;
        let ac = NodeSeqBuilder::shortened(a, ab, n)?;
        let bc = b.children().load(bb)?;
        children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = None;
        // children is just the shortened children a and b in the right order
        if ap[n] <= bp[n] {
            children.push_shortened_converted(a, ab, n)?;
            children.push_shortened_converted(b, bb, n)?;
        } else {
            children.push_shortened_converted(a, ab, n)?;
            children.push_shortened_converted(b, bb, n)?;
        }
    }
    target.push_new_unsplit(prefix, value, children, &NoStore)?;
    Ok(())
}

fn outer_combine_children<'a, A, B, E, F>(
    a: TreeChildrenIterator<'a, A>,
    ab: &A,
    b: TreeChildrenIterator<'a, B>,
    bb: &B,
    f: F,
) -> Result<NodeSeqBuilder, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<TreeValue>, E> + Copy,
{
    let mut res = NodeSeqBuilder::new();
    for x in OuterJoin::<A, B, E>::new(a, b) {
        match x? {
            (Some(a), Some(b)) => {
                outer_combine(&a, ab, &b, bb, f, &mut res)?;
            }
            (Some(a), None) => {
                res.push_detached(a, ab)?;
            }
            (None, Some(b)) => {
                res.push_detached(b, bb)?;
            }
            (None, None) => {}
        }
    }
    Ok(res)
}

fn inner_combine<A, B, E, F>(
    a: &TreeNode<A>,
    ab: &A,
    b: &TreeNode<B>,
    bb: &B,
    f: F,
    target: &mut NodeSeqBuilder,
) -> Result<(), E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueOptRef<A>, &TreeValueOptRef<B>) -> Result<Option<TreeValue>, E> + Copy,
{
    todo!()
}

fn inner_combine_children<'a, A, B, E, F>(
    a: TreeChildrenIterator<'a, A>,
    ab: &A,
    b: TreeChildrenIterator<'a, B>,
    bb: &B,
    f: F,
) -> Result<NodeSeqBuilder, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValueOptRef<A>, &TreeValueOptRef<B>) -> Result<Option<TreeValue>, E> + Copy,
{
    let mut res = NodeSeqBuilder::new();
    for x in OuterJoin::<A, B, E>::new(a, b) {
        match x? {
            (Some(a), Some(b)) => {
                inner_combine(&a, ab, &b, bb, f, &mut res)?;
            }
            (Some(a), None) => {}
            (None, Some(b)) => {}
            (None, None) => {}
        }
    }
    Ok(res)
}

struct OuterJoin<'a, A: BlobStore, B: BlobStore, E> {
    a: TreeChildrenIterator<'a, A>,
    b: TreeChildrenIterator<'a, B>,
    p: PhantomData<E>,
}

impl<'a, A, B, E> OuterJoin<'a, A, B, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error>,
{
    pub fn new(a: TreeChildrenIterator<'a, A>, b: TreeChildrenIterator<'a, B>) -> Self {
        Self {
            a,
            b,
            p: PhantomData,
        }
    }

    // get the next a, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_a(&mut self) -> Option<(Option<TreeNode<'a, A>>, Option<TreeNode<'a, B>>)> {
        let a = self.a.next()?.unwrap();
        Some((Some(a), None))
    }

    // get the next b, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_b(&mut self) -> Option<(Option<TreeNode<'a, A>>, Option<TreeNode<'a, B>>)> {
        let b = self.b.next()?.unwrap();
        Some((None, Some(b)))
    }

    // get the next a and b, assuming we know already that neither is an Err
    #[allow(clippy::type_complexity)]
    fn next_ab(&mut self) -> Option<(Option<TreeNode<'a, A>>, Option<TreeNode<'a, B>>)> {
        let a = self.a.next().transpose().unwrap();
        let b = self.b.next().transpose().unwrap();
        Some((a, b))
    }

    fn next0(&mut self) -> Result<Option<(Option<TreeNode<'a, A>>, Option<TreeNode<'a, B>>)>, E> {
        Ok(
            if let (Some(ak), Some(bk)) = (self.a.peek(), self.b.peek()) {
                match ak?.cmp(&bk?) {
                    Ordering::Less => self.next_a(),
                    Ordering::Greater => self.next_b(),
                    Ordering::Equal => self.next_ab(),
                }
            } else if let Some(a) = self.a.next() {
                Some((Some(a?), None))
            } else if let Some(b) = self.b.next() {
                Some((None, Some(b?)))
            } else {
                None
            },
        )
    }
}

impl<'a, A, B, E> Iterator for OuterJoin<'a, A, B, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error>,
{
    type Item = Result<(Option<TreeNode<'a, A>>, Option<TreeNode<'a, B>>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

struct Combiner<'a, A: BlobStore, B: BlobStore> {
    a: &'a mut InPlaceNodeSeqBuilder<A>,
    b: TreeChildrenIterator<'a, B>,
}

impl<'a, A, B> Combiner<'a, A, B>
where
    A: BlobStore,
    B: BlobStore,
    A::Error: From<B::Error>,
{
    pub fn new(a: &'a mut InPlaceNodeSeqBuilder<A>, b: TreeChildrenIterator<'a, B>) -> Self {
        Self { a, b }
    }

    /// Compare the first element of the lhs builder (a) with the first element of the rhs iterator (b)
    ///
    /// If this methond returns with success, it is guaranteed that neither a nor b produce an error
    fn cmp(&self) -> Result<Option<Ordering>, A::Error> {
        Ok(match (self.a.peek(), self.b.peek()) {
            (Some(a), Some(b)) => Some(a?.cmp(&b?)),
            (Some(a), None) => {
                let _ = a?;
                // take a
                Some(Ordering::Less)
            }
            (None, Some(b)) => {
                let _ = b?;
                // take b
                Some(Ordering::Greater)
            }
            (None, None) => None,
        })
    }
}

fn outer_combine_with<A, B, F>(
    a: InPlaceBuilderRef<A, AtPrefix>,
    ab: &A,
    b: &TreeNode<B>,
    bb: &B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore,
    B: BlobStore,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<TreeValue>, A::Error> + Copy,
{
    let ap = a.peek()?.load(ab)?;
    let bp = b.prefix().load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        let a = a.move_prefix()?;
        // prefixes are identical
        let a = match (a.peek().value_opt(), b.value.value_opt()) {
            (Some(av), Some(bv)) => {
                let r = f(av, bv)?;
                a.push_value_opt(r)?
            }
            (Some(_), None) => a.move_value()?,
            (None, Some(bv)) => a.insert_converted(bv, bb)?,
            (None, None) => a.push_value_none()?,
        };
        let bc = b.children().load(bb)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        let a = a.move_prefix()?.move_value()?;
        let bc = b.children().load(bb)?;
        outer_combine_children_with(a, &ab, bc.iter(), &bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        //     let ac = NodeSeqBuilder::shortened(a, ab, n)?;
        //     let bc = b.children().load(bb)?;
        //     children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else {
        //     // the two nodes are disjoint
        //     // value is none
        let a = a.move_prefix()?.push_value_none()?;
        //     value = None;
        //     // children is just the shortened children a and b in the right order
        //     if ap[n] <= bp[n] {
        //         children.push_shortened_converted(a, ab, n)?;
        //         children.push_shortened_converted(b, bb, n)?;
        //     } else {
        //         children.push_shortened_converted(a, ab, n)?;
        //         children.push_shortened_converted(b, bb, n)?;
        //     }
    }
    // target.push_new_unsplit(prefix, value, children, &NoStore)?;
    Ok(())
}

fn outer_combine_children_with<'a, A, B, F>(
    a: InPlaceBuilderRef<A, AtChildren>,
    ab: &A,
    bc: TreeChildrenIterator<'a, B>,
    bb: &B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore,
    B: BlobStore,
    A::Error: From<B::Error>,
    F: Fn(&TreeValueRef<A>, &TreeValueRef<B>) -> Result<Option<TreeValue>, A::Error> + Copy,
{
    let mut a_arc = a.take_arc(ab)?;
    let mut a_values = Arc::make_mut(&mut a_arc);
    let mut ac = InPlaceNodeSeqBuilder::<A>::new(&mut a_values);
    let mut c = Combiner::<A, B>::new(&mut ac, bc);
    while let Some(ordering) = c.cmp()? {
        match ordering {
            Ordering::Equal => {
                // the .unwrap().unwrap() is safe because cmp guarantees that there is a value, and it is not an error
                let b = c.b.next().unwrap().unwrap();
                outer_combine_with(c.a.cursor(), ab, &b, bb, f)?;
            }
            Ordering::Less => {
                c.a.move_one()?;
            }
            Ordering::Greater => {
                // the .unwrap().unwrap() is safe because cmp guarantees that there is a value, and it is not an error
                let b = c.b.next().unwrap().unwrap();
                c.a.insert_converted(b, bb)?;
            }
        }
    }
    *a_values = ac.into_inner();
    a.set_arc(a_arc)?;
    Ok(())
}

enum FindResult<T> {
    // Found an exact match
    Found(T),
    // found a tree for which the path is a prefix, with n remaining chars in the prefix of T
    Prefix {
        // a tree of which the searched path is a prefix
        tree: T,
        // number of remaining elements in the prefix of the tree
        rt: usize,
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
    tree: &TreeNode<S>,
    prefix: &[u8],
    f: impl Fn(FindResult<&TreeNode<S>>) -> Result<T, S::Error>,
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
        FindResult::Prefix { tree: tree, rt }
    } else if rt == 0 {
        // prefix is a subtree of tree
        let c = prefix[n];
        let tree_children = tree.children().load(store)?;
        if let Some(child) = tree_children.find(c)? {
            return find(store, &child, &prefix[n..], f);
        } else {
            FindResult::NotFound
        }
    } else {
        // disjoint, but we still need to store how far we matched
        FindResult::NotFound
    };
    f(fr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let a = Tree::single(b"aaaa", b"b");
        let b = Tree::single(b"aa", b"d");
        let r = a.outer_combine(&b, |_, b| Some(b.to_owned()));
        println!("{:?}", r);
        println!("{:?}", r.node.iter().next().unwrap().unwrap());

        let mut t = Tree::empty();
        for i in 0u64..1000 {
            let txt = i.to_string();
            let b = txt.as_bytes();
            t = t.outer_combine(&Tree::single(b, b), |_, b| Some(b.to_owned()))
        }
        println!("{:?}", t);
        println!("{:?}", t.node.iter().next().unwrap().unwrap());
        t.dump().unwrap();
        println!("{}", std::mem::size_of::<Arc<String>>());
        println!("{}", std::mem::size_of::<Option<Arc<String>>>());

        for i in 0..1000 {
            let res = t.try_get(i.to_string().as_bytes()).unwrap();
            println!("{:?}", res);
        }
    }
}
