use std::{cmp::Ordering, fmt, marker::PhantomData, ops::Deref, sync::Arc};

use crate::{
    store::{unwrap_safe, Blob2 as Blob, BlobStore2 as BlobStore, NoError, NoStore, OwnedBlob},
    Hex,
};
use std::fmt::Debug;

use super::{
    builders::{Extendable, NodeSeqBuilder},
    iterators::{NodeSeqIter, OwnedNodeSeqIter},
};

// trait DataStore: Clone {
//     type Error;
//     fn read_prefix(self, id: &[u8]) -> Result<(Self, OwnedBlob), Self::Error>;
//     fn read(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error>;
//     fn write_prefix(self, data: &[u8]) -> Result<(Self, OwnedBlob), Self::Error>;
//     fn write(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error>;
//     fn sync(&self) -> Result<(), Self::Error>;
// }

// impl DataStore for NoStore {
//     type Error = NoError;

//     fn read_prefix(self, id: &[u8]) -> Result<(Self, OwnedBlob), Self::Error> {
//         todo!()
//     }

//     fn read(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error> {
//         todo!()
//     }

//     fn write_prefix(self, data: &[u8]) -> Result<(Self, OwnedBlob), Self::Error> {
//         todo!()
//     }

//     fn write(&self, id: &[u8]) -> Result<OwnedBlob, Self::Error> {
//         todo!()
//     }

//     fn sync(&self) -> Result<(), Self::Error> {
//         todo!()
//     }
// }

#[repr(C)]
pub(crate) struct FlexRef<T>(PhantomData<T>, [u8]);

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
            Type::Ptr => write!(
                f,
                "FlexRef::Ptr({:x})",
                Arc::as_ptr(&self.arc_as_clone().unwrap()) as usize
            ),
        }
    }
}

impl<T> FlexRef<T> {
    pub fn arc_as_ref(&self) -> Option<&T> {
        self.with_arc(|arc| {
            let t: &T = arc.as_ref();
            // extend the lifetime
            unsafe { std::mem::transmute(t) }
        })
    }
}

impl FlexRef<Vec<u8>> {
    pub fn arc_vec_as_slice(&self) -> Option<&[u8]> {
        self.arc_as_ref().map(|x| x.as_ref())
    }

    pub fn first_u8_opt(&self) -> Option<u8> {
        match self.tpe() {
            Type::None => None,
            Type::Inline => self.1.get(1).cloned(),
            Type::Ptr => self.with_arc(|x| x.as_ref().get(0).cloned()).unwrap(),
            Type::Id => todo!("pack first byte into id"),
        }
    }

    pub fn first_u8(&self) -> u8 {
        match self.tpe() {
            Type::Inline => self.1[1],
            Type::Ptr => self.with_arc(|x| x[0]).unwrap(),
            Type::None => panic!(),
            Type::Id => todo!("pack first byte into id"),
        }
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
            // println!(
            //     "drop {:x} {}",
            //     Arc::as_ptr(arc) as usize,
            //     Arc::strong_count(arc)
            // );
            Arc::decrement_strong_count(Arc::as_ptr(arc));
        });
    }

    fn manual_clone(&self) {
        self.with_arc(|arc| unsafe {
            // println!(
            //     "clone {:x} {}",
            //     Arc::as_ptr(arc) as usize,
            //     Arc::strong_count(arc)
            // );
            Arc::increment_strong_count(Arc::as_ptr(arc));
        });
    }

    pub fn new(value: &[u8]) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn none() -> &'static Self {
        Self::read(&[NONE]).unwrap()
    }

    fn empty() -> &'static Self {
        Self::read(&[INLINE_EMPTY]).unwrap()
    }

    pub fn is_empty(&self) -> bool {
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

    pub fn arc_as_clone(&self) -> Option<Arc<T>> {
        self.with_arc(|x| x.clone())
    }

    pub fn id_as_slice(&self) -> Option<&[u8]> {
        if let Type::Id = self.tpe() {
            Some(self.data())
        } else {
            None
        }
    }

    pub fn ref_count(&self) -> usize {
        self.with_arc(|x| Arc::strong_count(x)).unwrap_or_default()
    }

    fn with_arc<U>(&self, f: impl Fn(&Arc<T>) -> U) -> Option<U> {
        if self.tpe() == Type::Ptr {
            let value = u64::from_be_bytes(self.1[1..9].try_into().unwrap());
            let value = usize::try_from(value).unwrap();
            // let arc: Arc<T> = unsafe { std::mem::transmute(value) };
            let arc: Arc<T> = unsafe { Arc::from_raw(value as *const T) };
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

    fn with_id<U>(&self, f: impl Fn(&[u8]) -> U) -> Option<U> {
        if self.tpe() == Type::Id {
            Some(f(self.data()))
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

pub(crate) const fn len(value: u8) -> usize {
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

pub(crate) const fn make_header_byte(tpe: Type, len: usize) -> u8 {
    if len >= 64 {
        assert!(len < 64);
    }
    (len as u8)
        | match tpe {
            Type::None => 0,
            Type::Inline => 1,
            Type::Ptr => 2,
            Type::Id => 3,
        } << 6
}

pub(crate) const NONE: u8 = make_header_byte(Type::None, 0);
pub(crate) const INLINE_EMPTY: u8 = make_header_byte(Type::Inline, 0);
pub(crate) const PTR8: u8 = make_header_byte(Type::Ptr, 8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Type {
    None,
    Inline,
    Id,
    Ptr,
}

#[repr(transparent)]
pub struct TreePrefixRef<S = NoStore>(PhantomData<S>, pub(crate) FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreePrefixRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreePrefix({:?})", &self.1)?;
        if let Some(x) = self.1.arc_as_ref() {
            write!(f, "Arc {}", Hex::partial(&x, 16))?;
        }
        Ok(())
    }
}

impl<S: BlobStore> TreePrefixRef<S> {
    pub fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    fn empty() -> &'static Self {
        Self::read(&[INLINE_EMPTY]).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.first_opt().is_none()
    }

    pub fn first_opt(&self) -> Option<u8> {
        self.1.first_u8_opt()
    }

    pub fn first(&self) -> u8 {
        self.1.first_u8()
    }

    pub(crate) fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    pub fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::None
    }

    pub fn load(&self, store: &S) -> Result<Blob, S::Error> {
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

    pub fn read(value: &[u8]) -> Option<&Self> {
        Some(Self::new(FlexRef::read(value)?))
    }

    fn read_one<'a>(value: &'a [u8]) -> Option<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Some((Self::new(f), rest))
    }

    pub(crate) fn manual_drop(&self) {
        self.1.manual_drop();
    }

    pub(crate) fn manual_clone(&self) {
        self.1.manual_clone();
    }

    pub(crate) fn arc_to_id(&self, store: &S) -> Result<Result<Vec<u8>, usize>, S::Error> {
        Ok(if let Some(data) = self.1.arc_vec_as_slice() {
            Ok(store.write(data)?)
        } else {
            Err(self.bytes().len())
        })
    }

    pub(crate) fn id_to_arc(&self, store: &S) -> Result<Result<OwnedBlob, usize>, S::Error> {
        Ok(if let Some(id) = self.1.id_as_slice() {
            Ok(store.read(id)?)
        } else {
            Err(self.bytes().len())
        })
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct TreeValueRefWrapper<S = NoStore>(OwnedBlob, PhantomData<S>);

impl<S: BlobStore> TreeValueRefWrapper<S> {
    pub fn new(blob: OwnedBlob) -> Self {
        Self(blob, PhantomData)
    }
}

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

impl<S: BlobStore> TreeValueRefWrapper<S> {
    pub fn load(&self, store: &S) -> Result<OwnedBlob, S::Error> {
        let t = TreeValueRef::<S>::new(FlexRef::new(self.0.as_ref()));
        Ok(t.load(store)?.to_owned())
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

    pub fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    pub fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::None
    }

    pub fn load(&self, store: &S) -> Result<Blob, S::Error> {
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
pub struct TreeValueOptRef<S: BlobStore = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreeValueOptRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeValueOpt({:?})", &self.1)
    }
}

impl<S: BlobStore> TreeValueOptRef<S> {
    pub(crate) fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    pub fn is_valid(&self) -> bool {
        self.bytes().len() > 0
    }

    pub fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    pub fn value_opt(&self) -> Option<&TreeValueRef<S>> {
        if self.is_none() {
            None
        } else {
            Some(TreeValueRef::new(&self.1))
        }
    }

    fn none() -> &'static Self {
        Self::new(FlexRef::new(&[NONE]))
    }

    pub fn is_none(&self) -> bool {
        self.1.is_none()
    }

    pub fn is_some(&self) -> bool {
        self.1.is_some()
    }

    pub fn load(&self, store: &S) -> Result<Option<Blob>, S::Error> {
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

    pub(crate) fn manual_drop(&self) {
        self.1.manual_drop();
    }

    pub(crate) fn manual_clone(&self) {
        self.1.manual_clone();
    }

    pub(crate) fn arc_to_id(&self, store: &S) -> Result<Result<Vec<u8>, usize>, S::Error> {
        Ok(if let Some(data) = self.1.arc_vec_as_slice() {
            Ok(store.write(data)?)
        } else {
            Err(self.bytes().len())
        })
    }

    pub(crate) fn id_to_arc(&self, store: &S) -> Result<Result<OwnedBlob, usize>, S::Error> {
        Ok(if let Some(id) = self.1.id_as_slice() {
            Ok(store.read(id)?)
        } else {
            Err(self.bytes().len())
        })
    }
}

#[repr(transparent)]
pub struct TreeChildrenRef<S: BlobStore>(PhantomData<S>, pub(crate) FlexRef<NodeSeqBuilder<S>>);

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
    pub(crate) fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn empty() -> &'static Self {
        Self::new(FlexRef::new(&[NONE]))
    }

    pub fn bytes(&self) -> &[u8] {
        self.1.bytes()
    }

    pub fn is_valid(&self) -> bool {
        self.bytes().len() > 0 && self.1.tpe() != Type::Inline
    }

    fn read_one<'a>(value: &'a [u8]) -> Option<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Some((Self::new(f), rest))
    }

    pub(crate) fn load_owned(&self, store: &S) -> Result<NodeSeq<'static, S>, S::Error> {
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

    pub(crate) fn load(&self, store: &S) -> Result<NodeSeq<'_, S>, S::Error> {
        Ok(if self.1.is_none() {
            NodeSeq::empty()
        } else if let Some(x) = self.1.arc_as_ref() {
            NodeSeq::from_blob(x.blob())
        } else if let Some(id) = self.1.id_as_slice() {
            NodeSeq::from_blob(store.read(id)?)
        } else {
            panic!()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.1.is_none()
    }

    pub(crate) fn manual_drop(&self) {
        self.1.manual_drop();
    }

    pub(crate) fn manual_clone(&self) {
        self.1.manual_clone();
    }
}

/// a newtype wrapper around a blob that contains a valid nodeseq
#[repr(transparent)]
pub(crate) struct NodeSeq<'a, S: BlobStore>(Blob<'a>, PhantomData<S>);

impl<S: BlobStore> NodeSeq<'static, S> {
    pub fn owned_iter(&self) -> OwnedNodeSeqIter<S> {
        OwnedNodeSeqIter::new(self.0.clone())
    }
}

impl<'a, S: BlobStore> NodeSeq<'a, S> {
    fn empty() -> Self {
        Self(Blob::empty(), PhantomData)
    }

    pub fn blob(&self) -> Blob<'a> {
        self.0.clone()
    }

    fn from_arc_vec(value: Arc<NodeSeqBuilder<S>>) -> Self {
        let data: &[u8] = value.as_ref().0.as_ref();
        // extend the lifetime
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        Self(Blob::owned_new(data, Some(value)), PhantomData)
    }

    fn from_blob(value: Blob<'a>) -> Self {
        Self(value, PhantomData)
    }

    pub fn iter(&self) -> NodeSeqIter<'_, S> {
        NodeSeqIter::new(&self.0)
    }

    pub fn find(&self, first: u8) -> Option<TreeNode<'_, S>> {
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

pub struct TreeNode<'a, S: BlobStore = NoStore> {
    pub(crate) prefix: &'a TreePrefixRef<S>,
    pub(crate) value: &'a TreeValueOptRef<S>,
    pub(crate) children: &'a TreeChildrenRef<S>,
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

    pub fn prefix(&self) -> &'a TreePrefixRef<S> {
        &self.prefix
    }

    pub fn value(&self) -> &'a TreeValueOptRef<S> {
        &self.value
    }

    pub fn children(&self) -> &'a TreeChildrenRef<S> {
        &self.children
    }

    pub fn is_empty(&self) -> bool {
        self.children().is_empty() && self.value().is_none()
    }

    pub fn read(buffer: &'a [u8]) -> Option<Self> {
        Some(Self::read_one(buffer)?.0)
    }

    pub fn read_one(buffer: &'a [u8]) -> Option<(Self, &'a [u8])> {
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

    pub(crate) fn manual_clone(&self) {
        self.prefix().manual_clone();
        self.value().manual_clone();
        self.children().manual_clone();
    }

    pub(crate) fn manual_drop(&self) {
        self.prefix().manual_drop();
        self.value().manual_drop();
        self.children().manual_drop();
    }

    pub fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error> {
        let spacer = std::iter::repeat(" ").take(indent).collect::<String>();
        let child_ref_count = self.children().1.ref_count();
        let child_count = self.children.load(store)?.iter().count();
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
        for child in self.children.load(store)?.iter() {
            child.dump(indent + 4, store)?;
        }
        Ok(())
    }

    /// get the first value
    pub fn first_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.value.is_some() {
            Some(TreeValueRefWrapper::new(Blob::copy_from_slice(self.value.bytes())))
        } else {
            let children = self.children.load(store)?;
            children.iter().next().unwrap().first_value(store)?
        })
    }

    /// get the first entry
    pub fn first_entry(
        &self,
        store: &S,
        mut prefix: Vec<u8>,
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        prefix.extend_from_slice(&self.prefix.load(store)?);
        Ok(if self.value.is_some() {
            Some((Blob::copy_from_slice(&prefix), TreeValueRefWrapper::new(Blob::copy_from_slice(self.value.bytes()))))
        } else {
            let children = self.children.load(store)?;
            children.iter().next().unwrap().first_entry(store, prefix)?
        })
    }

    /// get the last value
    pub fn last_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.children().is_empty() {
            self.value.value_opt().map(|x| TreeValueRefWrapper::new(Blob::copy_from_slice(x.bytes())))
        } else {
            let children = self.children.load(store)?;
            children.iter().last().unwrap().last_value(store)?
        })
    }

    /// get the first entry
    pub fn last_entry(
        &self,
        store: &S,
        mut prefix: Vec<u8>,
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        prefix.extend_from_slice(&self.prefix.load(store)?);
        Ok(if self.children().is_empty() {
            self.value.value_opt().map(|x| ((Blob::copy_from_slice(&prefix), TreeValueRefWrapper::new(Blob::copy_from_slice(x.bytes())))))
        } else {
            let c = self.children.load(store)?;
            c.iter().last().unwrap().last_entry(store, prefix)?
        })
    }

    pub fn validate(&self, store: &S) -> Result<bool, S::Error> {
        if self.prefix.1.ref_count() > 100 {
            return Ok(false);
        }
        if self.value.1.ref_count() > 100 {
            return Ok(false);
        }
        for child in self.children.load(store)?.iter() {
            if !child.validate(store)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
