use core::borrow;
use std::{
    any::{self, Any},
    cmp::Ordering,
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    mem::ManuallyDrop,
    num::NonZeroU8,
    ops::Deref,
    process::Child,
    sync::Arc,
    time::Instant,
};

use inplace_vec_builder::InPlaceVecBuilder;

use crate::{
    store::{
        unwrap_safe, Blob2 as Blob, BlobStore2 as BlobStore, MemStore2, NoError, NoStore, OwnedBlob,
    },
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
            Type::Inline => self.1.get(1).cloned(),
            Type::Id => self.1.get(1).cloned(),
            Type::Ptr => self.with_arc(|x| x.as_ref().get(0).cloned()).unwrap(),
            Type::None => None,
        }
    }
}

impl<T> FlexRef<T> {
    const fn header(&self) -> u8 {
        self.1[0]
    }

    fn data(&self) -> &[u8] {
        &self.1[1..]
    }

    fn bytes(&self) -> &[u8] {
        let len = len(self.header());
        &self.1
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
        if len(value[0]) + 1 != value.len() {
            debug_assert_eq!(len(value[0]) + 1, value.len());
        }
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
        let len = len(value[0]) + 1;
        if len > value.len() {
            return None;
        }
        Some((Self::new(&value[..len]), &value[len..]))
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
            store.read(&id[1..])?
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
            let mut id = store.write(data)?;
            id.insert(0, data.get(0).cloned().unwrap_or_default());
            Ok(id)
        } else {
            Err(self.bytes().len())
        })
    }

    pub(crate) fn id_to_arc(&self, store: &S) -> Result<Result<OwnedBlob, usize>, S::Error> {
        Ok(if let Some(id) = self.1.id_as_slice() {
            Ok(store.read(&id[1..])?)
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

    pub fn read(value: &[u8]) -> Option<&Self> {
        Some(Self::new(FlexRef::read(value)?))
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
            NodeSeq::from_blob(id[0] as usize, store.read(&id[1..])?)
        } else {
            panic!()
        })
    }

    pub(crate) fn load(&self, store: &S) -> Result<NodeSeq<'_, S>, S::Error> {
        Ok(if self.1.is_none() {
            NodeSeq::empty()
        } else if let Some(x) = self.1.arc_as_ref() {
            NodeSeq::from_blob(x.record_size, x.blob())
        } else if let Some(id) = self.1.id_as_slice() {
            NodeSeq::from_blob(id[0] as usize, store.read(&id[1..])?)
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
pub(crate) struct NodeSeq<'a, S: BlobStore> {
    data: Blob<'a>,
    record_size: usize,
    p: PhantomData<S>,
}

impl<S: BlobStore> NodeSeq<'static, S> {
    pub fn owned_iter(&self) -> OwnedNodeSeqIter<S> {
        OwnedNodeSeqIter::new(self.data.clone())
    }
}

impl<'a, S: BlobStore> NodeSeq<'a, S> {
    fn empty() -> Self {
        Self {
            data: Blob::empty(),
            record_size: 0,
            p: PhantomData,
        }
    }

    pub fn blob(&self) -> &Blob<'a> {
        &self.data
    }

    fn from_arc_vec(value: Arc<NodeSeqBuilder<S>>) -> Self {
        let data: &[u8] = value.as_ref().data.as_ref();
        let record_size = value.as_ref().record_size;
        // extend the lifetime
        let data: &'static [u8] = unsafe { std::mem::transmute(data) };
        Self {
            data: Blob::owned_new(data, Some(value)),
            record_size,
            p: PhantomData,
        }
    }

    fn from_blob(record_size: usize, data: Blob<'a>) -> Self {
        Self {
            data,
            record_size,
            p: PhantomData,
        }
    }

    pub fn iter(&self) -> NodeSeqIter<'_, S> {
        NodeSeqIter::new(&self.data)
    }

    pub fn find(&self, elem: u8) -> Option<TreeNode<'_, S>> {
        if self.record_size != 0 {
            let records = self.data.len() / self.record_size;
            if records == 256 {
                let offset = (elem as usize) * self.record_size;
                return TreeNode::read(&self.data[offset..]);
            }
        }
        for leaf in self.iter() {
            let first_opt = leaf.prefix().first_opt();
            if first_opt == Some(elem) {
                // found it
                return Some(leaf);
            } else if first_opt > Some(elem) {
                // not going to come anymore
                return None;
            }
        }
        None
    }
}

const PTR_SIZE: usize = std::mem::size_of::<*const u8>();

union ArcOrInlineBlob {
    arc: ManuallyDrop<Arc<Vec<u8>>>,
    inline: [u8; PTR_SIZE],
}

impl ArcOrInlineBlob {
    const EMPTY: Self = Self {
        inline: [0u8; PTR_SIZE],
    };

    fn copy_from_slice(data: &[u8]) -> Self {
        if data.len() > PTR_SIZE {
            let arc = Arc::new(data.to_vec());
            Self {
                arc: ManuallyDrop::new(arc),
            }
        } else {
            let mut inline = [0u8; PTR_SIZE];
            inline[0..data.len()].copy_from_slice(data);
            Self { inline }
        }
    }

    fn ref_count(&self, hdr: Header) -> Option<usize> {
        if hdr.len() <= PTR_SIZE {
            None
        } else {
            Some(Arc::strong_count(unsafe { &self.arc }))
        }
    }

    fn slice(&self, hdr: Header) -> &[u8] {
        let len = hdr.len();
        unsafe {
            if len <= PTR_SIZE {
                &self.inline[..len]
            } else {
                self.arc.as_ref().as_ref()
            }
        }
    }

    fn manual_clone(&self, hdr: Header) -> Self {
        unsafe {
            if hdr.len() > PTR_SIZE {
                Self {
                    arc: self.arc.clone(),
                }
            } else {
                Self {
                    inline: self.inline,
                }
            }
        }
    }

    fn manual_drop(&mut self, hdr: Header) {
        unsafe {
            if hdr.len() > PTR_SIZE {
                ManuallyDrop::drop(&mut self.arc);
            }
        }
        if cfg!(debug_assertions) {
            // just to be on the safe side, dropping twice will be like dropping a null ptr
            self.inline = [0; PTR_SIZE];
        }
    }
}

union ChildrenRef<S> {
    arc_id: ManuallyDrop<Arc<Vec<u8>>>,
    arc_data: ManuallyDrop<Arc<Vec<OwnedTreeNode<S>>>>,
    inline: [u8; PTR_SIZE],
    p: PhantomData<S>,
}

impl<S> ChildrenRef<S> {
    const EMPTY: Self = Self {
        inline: [0u8; PTR_SIZE],
    };

    fn ref_count(&self, hdr: Header) -> Option<usize> {
        if hdr.len() > PTR_SIZE {
            Some(if hdr.is_id() {
                Arc::strong_count(unsafe { &self.arc_id })
            } else {
                Arc::strong_count(unsafe { &self.arc_data })
            })
        } else {
            None
        }
    }

    fn id_from_slice(data: &[u8]) -> Self {
        if data.len() > PTR_SIZE {
            let arc = Arc::new(data.to_vec());
            Self {
                arc_id: ManuallyDrop::new(arc),
            }
        } else {
            let mut inline = [0u8; PTR_SIZE];
            inline[0..data.len()].copy_from_slice(data);
            Self { inline }
        }
    }

    fn data_from_arc(arc: Arc<Vec<OwnedTreeNode<S>>>) -> Self {
        Self {
            arc_data: ManuallyDrop::new(arc),
        }
    }

    fn deref(&self, hdr: Header) -> Result<&Arc<Vec<OwnedTreeNode<S>>>, &[u8]> {
        let len = hdr.len();
        unsafe {
            if hdr.is_id() {
                Err(if len <= PTR_SIZE {
                    &self.inline[..len]
                } else {
                    self.arc_id.as_ref().as_ref()
                })
            } else {
                assert!(len > PTR_SIZE);
                Ok(&self.arc_data)
            }
        }
    }

    fn deref_mut(&mut self, hdr: Header) -> Result<&mut Arc<Vec<OwnedTreeNode<S>>>, &[u8]> {
        let len = hdr.len();
        unsafe {
            if hdr.is_id() {
                Err(if len <= PTR_SIZE {
                    &self.inline[..len]
                } else {
                    self.arc_id.as_ref().as_ref()
                })
            } else {
                assert!(len > PTR_SIZE);
                Ok(&mut self.arc_data)
            }
        }
    }

    fn manual_clone(&self, hdr: Header) -> Self {
        unsafe {
            if hdr.len() > PTR_SIZE {
                if hdr.is_id() {
                    Self {
                        arc_id: self.arc_id.clone(),
                    }
                } else {
                    Self {
                        arc_data: self.arc_data.clone(),
                    }
                }
            } else {
                Self {
                    inline: self.inline,
                }
            }
        }
    }

    fn manual_drop(&mut self, hdr: Header) {
        unsafe {
            if hdr.len() > PTR_SIZE {
                if hdr.is_id() {
                    ManuallyDrop::drop(&mut self.arc_id);
                } else {
                    ManuallyDrop::drop(&mut self.arc_data);
                }
            }
        }
        if cfg!(debug_assertions) {
            // just to be on the safe side, dropping twice will be like dropping a null ptr
            self.inline = [0; PTR_SIZE];
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Header(u8);

impl Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_none() {
            write!(f, "Header::None")
        } else if self.is_id() {
            write!(f, "Header::Id({})", self.len())
        } else {
            write!(f, "Header::Data({})", self.len())
        }
    }
}

impl From<u8> for Header {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<Header> for u8 {
    fn from(value: Header) -> Self {
        value.0
    }
}

impl Header {
    const EMPTY: Header = Header::data(0);
    const NONE: Header = Header::id(0);
    const ARCDATA: Header = Header::data(0x7f);

    const fn id(len: usize) -> Self {
        assert!(len < 0x80);
        Self((len as u8) | 0x80)
    }

    const fn data(len: usize) -> Self {
        if len <= 0x7f {
            Self(len as u8)
        } else {
            Self(0x7f)
        }
    }

    fn is_id(&self) -> bool {
        (self.0 & 0x80) != 0
    }

    fn is_data(&self) -> bool {
        !self.is_id()
    }

    fn is_none(&self) -> bool {
        self.0 == 0x80
    }

    /// Note that this is the length that fits in the header.
    ///
    /// The actual length can be larger!
    fn len(&self) -> usize {
        self.len_u8() as usize
    }

    fn len_u8(&self) -> u8 {
        self.0 & 0x7f
    }
}

#[derive(Clone, Copy)]
struct Raw<'a> {
    hdr: Header,
    data: &'a ArcOrInlineBlob,
}

impl<'a> Debug for Raw<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_none() {
            write!(f, "None")
        } else if self.is_id() {
            write!(f, "Id{}", Hex::new(self.slice()))
        } else {
            write!(f, "Data{}", Hex::new(self.slice()))
        }
    }
}

impl<'a> Raw<'a> {
    const EMPTY: Raw<'static> = Raw {
        hdr: Header::EMPTY,
        data: &ArcOrInlineBlob::EMPTY,
    };

    fn ref_count(&self) -> Option<usize> {
        self.data.ref_count(self.hdr)
    }

    fn new(hdr: Header, data: &'a ArcOrInlineBlob) -> Self {
        Self { hdr, data }
    }

    fn is_id(&self) -> bool {
        self.hdr.is_id()
    }

    fn is_none(&self) -> bool {
        self.hdr.is_none()
    }

    fn slice(&self) -> &'a [u8] {
        self.data.slice(self.hdr)
    }

    fn serialize<S: BlobStore>(&self, target: &mut Vec<u8>, store: &S) -> Result<(), S::Error> {
        let slice = self.slice();
        if self.is_id() || slice.len() < 0x80 {
            target.push(self.hdr.into());
            target.extend_from_slice(slice);
        } else {
            let id = store.write(slice)?;
            target.push(Header::id(id.len()).into());
            target.extend_from_slice(&id);
        }
        Ok(())
    }
}

struct IdOrData<'a> {
    hdr: Header,
    data: &'a u8,
}

impl<'a> Debug for IdOrData<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.read() {
            Ok(value) => {
                write!(f, "Data{}", Hex::new(value))
            }
            Err(id) => {
                if id.is_empty() {
                    write!(f, "None")
                } else {
                    write!(f, "Id{}", Hex::new(id))
                }
            }
        }
    }
}

impl<'a> IdOrData<'a> {
    fn new(hdr: Header, data: &'a u8) -> Self {
        Self { hdr, data }
    }

    fn is_id(&self) -> bool {
        self.hdr.is_id()
    }

    fn is_none(&self) -> bool {
        self.hdr.is_none()
    }

    fn bytes(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.hdr.len() + 1) }
    }

    fn slice(&self) -> &'a [u8] {
        &self.bytes()[1..]
    }

    /// OK is data, Err is id
    fn read(&self) -> Result<&'a [u8], &'a [u8]> {
        if !self.is_id() {
            Ok(self.slice())
        } else {
            Err(self.slice())
        }
    }
}

enum ValueRef<'a, S> {
    Raw(Raw<'a>, PhantomData<S>),
    IdOrData(IdOrData<'a>),
}

impl<'a> ValueRef<'a, NoStore> {
    fn downcast<S2: BlobStore>(&self) -> &ValueRef<'a, S2> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a, S> ValueRef<'a, S> {
    fn to_owned(&self) -> OwnedValue<S> {
        match self {
            Self::Raw(x, _) => OwnedValueRef::new(*x).to_owned(),
            Self::IdOrData(x) => OwnedValue {
                hdr: x.hdr,
                data: ArcOrInlineBlob::copy_from_slice(x.slice()),
                p: PhantomData,
            },
        }
    }

    fn is_id(&self) -> bool {
        match self {
            Self::Raw(x, _) => x.is_id(),
            Self::IdOrData(x) => x.is_id(),
        }
    }
    fn is_none(&self) -> bool {
        match self {
            Self::Raw(x, _) => x.is_none(),
            Self::IdOrData(x) => x.is_none(),
        }
    }
    fn slice(&self) -> &'a [u8] {
        match self {
            Self::Raw(x, _) => x.slice(),
            Self::IdOrData(x) => x.slice(),
        }
    }
}

pub struct OwnedValueRef<'a, S> {
    raw: Raw<'a>,
    p: PhantomData<S>,
}

impl<'a> OwnedValueRef<'a, NoStore> {
    fn downcast<S2: BlobStore>(&self) -> &OwnedValueRef<'a, S2> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a, S> OwnedValueRef<'a, S> {
    fn new(raw: Raw<'a>) -> Self {
        Self {
            raw,
            p: PhantomData,
        }
    }
    fn to_owned(&self) -> OwnedValue<S> {
        OwnedValue {
            hdr: self.raw.hdr,
            data: self.raw.data.manual_clone(self.raw.hdr),
            p: PhantomData,
        }
    }
}

pub struct OwnedValue<S> {
    hdr: Header,
    data: ArcOrInlineBlob,
    p: PhantomData<S>,
}

impl<S> OwnedValue<S> {
    const EMPTY: Self = Self {
        hdr: Header::NONE,
        data: ArcOrInlineBlob::EMPTY,
        p: PhantomData,
    };

    fn as_ref(&self) -> OwnedValueRef<S> {
        OwnedValueRef::new(Raw {
            hdr: self.hdr,
            data: &self.data,
        })
    }

    fn read(&self) -> Result<&[u8], &[u8]> {
        if self.hdr.is_data() {
            Ok(self.data.slice(self.hdr))
        } else {
            Err(self.data.slice(self.hdr))
        }
    }
}

impl<S> Drop for OwnedValue<S> {
    fn drop(&mut self) {
        self.data.manual_drop(self.hdr);
    }
}

#[repr(C)]
pub struct OwnedTreeNode<S> {
    discriminator: u8,
    prefix_hdr: Header,
    value_hdr: Header,
    children_hdr: Header,
    prefix: ArcOrInlineBlob,
    value: ArcOrInlineBlob,
    children: ChildrenRef<S>,
}

impl<S: BlobStore> OwnedTreeNode<S> {
    fn as_ref(&self) -> TreeNodeRef<S> {
        TreeNodeRef::Owned(self)
    }
}

impl OwnedTreeNode<NoStore> {
    fn downcast<S2: BlobStore>(&self) -> OwnedTreeNode<S2> {
        let res = self.clone();
        unsafe { std::mem::transmute(res) }
    }

    fn downcast_ref<S2: BlobStore>(&self) -> &OwnedTreeNode<S2> {
        unsafe { std::mem::transmute(&self) }
    }
}

impl<S> Debug for OwnedTreeNode<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedTreeNode")
            .field("prefix", &self.prefix())
            .field("value", &self.value())
            .field("children", &self.get_children())
            .finish()
    }
}

impl<S: BlobStore> OwnedTreeNode<S> {
    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error> {
        let spacer = std::iter::repeat(" ").take(indent).collect::<String>();
        let child_ref_count = self.children.ref_count(self.children_hdr);
        let format_ref_count =
            |x: Option<usize>| x.map(|n| format!(" rc={}", n)).unwrap_or_default();
        println!("{}TreeNode", spacer);
        println!(
            "{}  prefix={:?}{}",
            spacer,
            self.prefix(),
            format_ref_count(self.prefix().ref_count())
        );
        println!(
            "{}  value={:?}{}",
            spacer,
            self.value(),
            format_ref_count(self.value().ref_count())
        );
        let mut iter = self.load_children(store)?;
        if !iter.is_empty() {
            println!("{}  children {}", spacer, format_ref_count(child_ref_count));
            while let Some(child) = iter.next() {
                child.dump(indent + 4, store)?;
            }
        }
        Ok(())
    }

    fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        if let Some((node, rest)) = BorrowedTreeNode::<S>::read_one(data) {
            anyhow::ensure!(rest.is_empty());
            let mut res = Self::EMPTY;
            res.set_prefix_id_or_data(node.prefix());
            res.set_value_id_or_data(node.value());
            res.set_children_id(node.children().read().unwrap_err());
            Ok(res)
        } else {
            anyhow::bail!("Unable to deserialize {}!", Hex::new(data));
        }
    }

    fn serialize<S2: BlobStore>(&self, target: &mut Vec<u8>, store: &S2) -> Result<(), S2::Error> {
        self.prefix().serialize(target, store)?;
        self.value().serialize(target, store)?;
        match self.get_children() {
            Ok(children) => {
                assert!(!children.is_empty());
                let mut serialized = Vec::new();
                for child in children.iter() {
                    child.serialize(&mut serialized, store)?;
                }
                let id = store.write(&serialized)?;
                target.push(Header::id(id.len()).into());
                target.extend_from_slice(&id);
            }
            Err(id) => {
                target.push(self.children_hdr.into());
                target.extend_from_slice(id);
            }
        }
        Ok(())
    }

    /// True if key is contained in this set
    fn contains_key(&self, key: &[u8], store: &S) -> Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(store, &TreeNodeRef::Owned(&self), key, |r| {
            Ok(if let FindResult::Found(tree) = r {
                tree.value_opt().is_some()
            } else {
                false
            })
        })
    }

    fn get(&self, key: &[u8], store: &S) -> Result<Option<OwnedValue<S>>, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(store, &TreeNodeRef::Owned(&self), key, |r| {
            Ok(if let FindResult::Found(tree) = r {
                tree.value_opt().map(|x| x.to_owned())
            } else {
                None
            })
        })
    }

    fn load_children(&self, store: &S) -> Result<TreeNodeIter<S>, S::Error> {
        match self.get_children() {
            Ok(children) => Ok(TreeNodeIter::Owned(children.iter())),
            Err(id) => TreeNodeIter::load(id, store),
        }
    }

    fn load_children_mut(&mut self, store: &S) -> Result<&mut Vec<OwnedTreeNode<S>>, S::Error> {
        if let Err(id) = self.get_children() {
            let mut iter = TreeNodeIter::load(id, store)?;
            let mut items = Vec::new();
            while let Some(item) = iter.next() {
                items.push(item.to_owned());
            }
            self.set_children_arc(Arc::new(items));
        };
        let children = self.get_children_mut().unwrap();
        Ok(Arc::make_mut(children))
    }

    fn clone_shortened(&self, store: &S, n: usize) -> Result<Self, S::Error> {
        let prefix = self.load_prefix(store)?;
        let mut res = self.clone();
        res.set_prefix_slice(&prefix[n..]);
        Ok(res)
    }

    fn split(&mut self, store: &S, n: usize) -> Result<(), S::Error> {
        // todo: get rid of the to_vec!
        let prefix = self.load_prefix(store)?.to_vec();
        let mut child = Self::EMPTY;
        child.set_prefix_slice(&prefix[n..]);
        self.set_prefix_slice(&prefix[..n]);
        // child.childen = self.children, self.children = empty
        std::mem::swap(&mut self.children_hdr, &mut child.children_hdr);
        std::mem::swap(&mut self.children, &mut child.children);
        // child.value = self.value, self.value = none
        std::mem::swap(&mut self.value, &mut child.value);
        std::mem::swap(&mut self.value_hdr, &mut child.value_hdr);
        // now, self is a degenerate empty node with first being the prefix
        // child is the former self (value and children) with rest as prefix
        self.set_children_arc(Arc::new(vec![child]));
        Ok(())
    }

    fn canonicalize(&mut self) {
        let cc = self.child_count();
        if !self.has_value() && cc == 1 {
            let children = self.get_children_mut().expect("children must be loaded");
            let children = Arc::make_mut(children);
            let mut child = children.pop().unwrap();
            debug_assert!(!self.prefix().is_id(), "prefix must be loaded");
            debug_assert!(!child.prefix().is_id(), "child prefix must be loaded");
            // combine the prefix again
            let mut prefix = self.prefix().slice().to_vec();
            prefix.extend_from_slice(child.prefix().slice());
            self.set_prefix_slice(&prefix);
            // take value from child
            std::mem::swap(&mut self.value_hdr, &mut child.value_hdr);
            std::mem::swap(&mut self.value, &mut child.value);
            // take children from child
            std::mem::swap(&mut self.children_hdr, &mut child.children_hdr);
            std::mem::swap(&mut self.children, &mut child.children);
        }
        if !self.has_value() && self.child_count() == 0 {
            self.set_prefix_raw(Raw::EMPTY);
            self.children_hdr = Header::NONE;
            self.children = ChildrenRef::EMPTY;
        }
    }
}

impl<S> OwnedTreeNode<S> {
    const EMPTY: Self = Self {
        discriminator: 0,
        prefix_hdr: Header::EMPTY,
        prefix: ArcOrInlineBlob::EMPTY,
        value_hdr: Header::NONE,
        value: ArcOrInlineBlob::EMPTY,
        children_hdr: Header::NONE,
        children: ChildrenRef::EMPTY,
    };

    fn first_prefix_byte(&self) -> Option<u8> {
        self.prefix().slice().get(0).cloned()
    }

    fn single(prefix: &[u8], value: &[u8]) -> Self {
        let mut res = Self::EMPTY;
        res.set_prefix_slice(prefix);
        res.set_value_slice(Some(value));
        res
    }

    fn leaf(value: &[u8]) -> Self {
        let mut res = Self::EMPTY;
        res.set_value_slice(Some(value));
        res
    }

    fn prefix(&self) -> Raw<'_> {
        Raw::new(self.prefix_hdr, &self.prefix)
    }

    fn value(&self) -> Raw<'_> {
        Raw::new(self.value_hdr, &self.value)
    }

    fn is_empty(&self) -> bool {
        !self.has_value() && self.child_count() == 0
    }

    fn set_prefix_raw(&mut self, prefix: Raw) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = prefix.hdr;
        self.prefix = prefix.data.manual_clone(prefix.hdr);
    }

    fn set_value_raw(&mut self, value: Raw) {
        self.value.manual_drop(self.value_hdr);
        self.value_hdr = value.hdr;
        self.value = value.data.manual_clone(value.hdr);
    }

    fn set_prefix_id_or_data(&mut self, prefix: IdOrData) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = prefix.hdr;
        self.prefix = ArcOrInlineBlob::copy_from_slice(prefix.slice());
    }

    fn set_value_id_or_data(&mut self, value: IdOrData) {
        self.value.manual_drop(self.value_hdr);
        self.value_hdr = value.hdr;
        self.value = ArcOrInlineBlob::copy_from_slice(value.slice());
    }

    fn set_prefix_slice(&mut self, prefix: &[u8]) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = Header::data(prefix.len());
        self.prefix = ArcOrInlineBlob::copy_from_slice(prefix);
    }

    fn set_value_slice(&mut self, value: Option<&[u8]>) {
        self.value.manual_drop(self.value_hdr);
        if let Some(value) = value {
            self.value_hdr = Header::data(value.len());
            self.value = ArcOrInlineBlob::copy_from_slice(value);
        } else {
            self.value_hdr = Header::NONE;
            self.value = ArcOrInlineBlob::EMPTY;
        }
    }

    fn get_children(&self) -> Result<&Arc<Vec<OwnedTreeNode<S>>>, &[u8]> {
        self.children.deref(self.children_hdr)
    }

    fn get_children_mut(&mut self) -> Result<&mut Arc<Vec<OwnedTreeNode<S>>>, &[u8]> {
        self.children.deref_mut(self.children_hdr)
    }

    fn set_children_arc(&mut self, arc: Arc<Vec<OwnedTreeNode<S>>>) {
        self.children.manual_drop(self.children_hdr);
        self.children_hdr = Header::ARCDATA;
        self.children = ChildrenRef::data_from_arc(arc);
    }

    fn set_children_id(&mut self, id: &[u8]) {
        self.children.manual_drop(self.children_hdr);
        self.children_hdr = Header::id(id.len());
        self.children = ChildrenRef::id_from_slice(id);
    }

    fn load_prefix(&self, store: &S) -> Result<Blob<'_>, S::Error>
    where
        S: BlobStore,
    {
        if self.prefix().is_id() {
            store.read(self.prefix().slice())
        } else {
            Ok(Blob::new(self.prefix().slice()))
        }
    }

    fn load_value(&self, store: &S) -> Result<Option<Blob<'_>>, S::Error>
    where
        S: BlobStore,
    {
        if self.value().is_id() {
            if self.value().is_none() {
                Ok(None)
            } else {
                store.read(self.value().slice()).map(Some)
            }
        } else {
            Ok(Some(Blob::new(self.value().slice())))
        }
    }

    fn has_value(&self) -> bool {
        self.value_hdr != Header::NONE
    }

    fn value_opt(&self) -> Option<OwnedValueRef<'_, S>> {
        if self.has_value() {
            Some(OwnedValueRef::new(self.value()))
        } else {
            None
        }
    }

    fn take_value_opt(&mut self) -> Option<OwnedValue<S>> {
        if self.has_value() {
            let mut value = OwnedValue::EMPTY;
            std::mem::swap(&mut self.value_hdr, &mut value.hdr);
            std::mem::swap(&mut self.value, &mut value.data);
            Some(value)
        } else {
            None
        }
    }

    fn take_value(&mut self) -> OwnedValue<S> {
        let mut value = OwnedValue::EMPTY;
        std::mem::swap(&mut self.value_hdr, &mut value.hdr);
        std::mem::swap(&mut self.value, &mut value.data);
        value
    }

    fn is_leaf(&self) -> bool {
        self.children_hdr == Header::NONE
    }

    fn child_count(&self) -> usize {
        match self.get_children() {
            Ok(data) => data.len(),
            Err(id) => {
                if id.is_empty() {
                    0
                } else {
                    panic!()
                }
            }
        }
    }
}

impl<S> Drop for OwnedTreeNode<S> {
    fn drop(&mut self) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.value.manual_drop(self.value_hdr);
        self.children.manual_drop(self.children_hdr);
    }
}

impl<S> Clone for OwnedTreeNode<S> {
    fn clone(&self) -> Self {
        Self {
            discriminator: 0,
            prefix_hdr: self.prefix_hdr,
            value_hdr: self.value_hdr,
            children_hdr: self.children_hdr,
            prefix: self.prefix.manual_clone(self.prefix_hdr),
            value: self.value.manual_clone(self.value_hdr),
            children: self.children.manual_clone(self.children_hdr),
        }
    }
}

#[repr(C)]
pub struct BorrowedTreeNode<'a, S> {
    discriminator: u8,
    prefix_hdr: Header,
    value_hdr: Header,
    children_hdr: Header,
    prefix: &'a u8,
    value: &'a u8,
    children: &'a u8,
    p: PhantomData<&'a S>,
}

impl<'a, S> Clone for BorrowedTreeNode<'a, S> {
    fn clone(&self) -> Self {
        Self {
            discriminator: 1,
            prefix_hdr: self.prefix_hdr,
            value_hdr: self.value_hdr,
            children_hdr: self.children_hdr,
            prefix: self.prefix,
            value: self.value,
            children: self.children,
            p: self.p,
        }
    }
}

impl<'a, S> Copy for BorrowedTreeNode<'a, S> {}

impl<'a, S> Debug for BorrowedTreeNode<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedTreeNode")
            .field("prefix", &self.prefix())
            .field("value", &self.value())
            .field("children_hdr", &self.children())
            .finish()
    }
}

const EMPTY_BYTES: &'static [u8] = &[Header::EMPTY.0, Header::NONE.0, Header::NONE.0];

impl<S: 'static> BorrowedTreeNode<'static, S> {
    const EMPTY: Self = Self {
        discriminator: 1,
        prefix: &EMPTY_BYTES[0],
        value: &EMPTY_BYTES[1],
        children: &EMPTY_BYTES[2],
        prefix_hdr: Header::EMPTY,
        value_hdr: Header::NONE,
        children_hdr: Header::NONE,
        p: PhantomData,
    };
}

impl<'a, S> BorrowedTreeNode<'a, S> {
    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error>
    where
        S: BlobStore,
    {
        let spacer = std::iter::repeat(" ").take(indent).collect::<String>();
        println!("{}TreeNode", spacer);
        println!("{}  prefix={:?}", spacer, self.prefix(),);
        println!("{}  value={:?}", spacer, self.value(),);
        let mut iter = self.load_children(store)?;
        println!("{}", spacer);
        while let Some(child) = iter.next() {
            child.dump(indent + 4, store)?;
        }
        Ok(())
    }

    fn first_prefix_byte(&self) -> Option<u8> {
        self.prefix().slice().get(0).cloned()
    }

    fn prefix(&self) -> IdOrData<'a> {
        IdOrData::new(self.prefix_hdr, self.prefix)
    }

    fn value(&self) -> IdOrData<'a> {
        IdOrData::new(self.value_hdr, self.value)
    }

    fn children(&self) -> IdOrData<'a> {
        IdOrData::new(self.children_hdr, self.children)
    }

    fn bytes_len(&self) -> usize {
        self.prefix_hdr.len() + self.value_hdr.len() + self.children_hdr.len() + 3
    }

    fn load_prefix(&self, store: &S) -> Result<Blob, S::Error>
    where
        S: BlobStore,
    {
        match self.prefix().read() {
            Ok(data) => Ok(Blob::new(data)),
            Err(id) => store.read(id),
        }
    }

    fn value_opt(&self) -> Option<ValueRef<S>> {
        if self.value_hdr != Header::NONE {
            Some(ValueRef::IdOrData(self.value()))
        } else {
            None
        }
    }

    fn load_children(&self, store: &S) -> Result<TreeNodeIter<S>, S::Error>
    where
        S: BlobStore,
    {
        Ok(if self.children_hdr.is_none() {
            TreeNodeIter::Owned([].iter())
        } else {
            assert!(self.children_hdr.is_id());
            let id = self.children().slice();
            let blob = store.read(id)?;
            TreeNodeIter::Borrowed(blob, 0)
        })
    }

    pub fn read(buffer: &'a [u8]) -> Option<Self> {
        Some(Self::read_one(buffer)?.0)
    }

    pub fn read_one(rest: &'a [u8]) -> Option<(Self, &'a [u8])> {
        let prefix_hdr = Header::from(*rest.get(0)?);
        let len = prefix_hdr.len() + 1;
        if rest.len() < len {
            return None;
        }
        let (prefix, rest) = (&rest[0], &rest[len..]);

        let value_hdr = Header::from(*rest.get(0)?);
        let len = value_hdr.len() + 1;
        if rest.len() < len {
            return None;
        }
        let (value, rest) = (&rest[0], &rest[len..]);

        let children_hdr = Header::from(*rest.get(0)?);
        let len = children_hdr.len() + 1;
        if rest.len() < len {
            return None;
        }
        let (children, rest) = (&rest[0], &rest[len..]);

        Some((
            Self {
                discriminator: 1,
                prefix_hdr,
                prefix,
                value_hdr,
                value,
                children_hdr,
                children,
                p: PhantomData,
            },
            &rest,
        ))
    }
}

#[derive(Clone, Copy)]
pub enum TreeNodeRef<'a, S: BlobStore = NoStore> {
    Owned(&'a OwnedTreeNode<S>),
    Borrowed(BorrowedTreeNode<'a, S>),
}

impl<'a, S: BlobStore> Debug for TreeNodeRef<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.dispatch() {
            Ok(owned) => f.debug_tuple("Owned").field(owned).finish(),
            Err(borrowed) => f.debug_tuple("Borrowed").field(borrowed).finish(),
        }
    }
}

impl<'a> TreeNodeRef<'a, NoStore> {
    fn downcast<S2: BlobStore>(&self) -> OwnedTreeNode<S2> {
        match self.dispatch() {
            Ok(owned) => owned.downcast(),
            Err(borrowed) => todo!(),
        }
    }
}

impl<'a, S: BlobStore> TreeNodeRef<'a, S> {
    fn dispatch(&self) -> Result<&'a OwnedTreeNode<S>, &'a BorrowedTreeNode<S>> {
        match self {
            Self::Owned(inner) => Ok(inner),
            Self::Borrowed(inner) => Err(inner),
        }
    }

    fn to_owned(self) -> OwnedTreeNode<S> {
        match self.dispatch() {
            Ok(owned) => owned.clone(),
            Err(borrowed) => todo!(),
        }
    }

    fn dump(self, indent: usize, store: &S) -> Result<(), S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.dump(indent, store),
            Err(borrowed) => borrowed.dump(indent, store),
        }
    }

    fn load_prefix(&self, store: &S) -> Result<Blob, S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.load_prefix(store),
            Err(borrowed) => borrowed.load_prefix(store),
        }
    }

    fn load_children(&self, store: &S) -> Result<TreeNodeIter<S>, S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.load_children(store),
            Err(borrowed) => borrowed.load_children(store),
        }
    }

    fn value_opt(&self) -> Option<ValueRef<S>> {
        match self.dispatch() {
            Ok(owned) => owned.value_opt().map(|x| ValueRef::Raw(x.raw, PhantomData)),
            Err(borrowed) => borrowed.value_opt(),
        }
    }

    fn first_prefix_byte(&self) -> Option<u8> {
        match self.dispatch() {
            Ok(owned) => owned.first_prefix_byte(),
            Err(borrowed) => borrowed.first_prefix_byte(),
        }
    }

    fn clone_shortened(&self, store: &S, n: usize) -> Result<OwnedTreeNode<S>, S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.clone_shortened(store, n),
            Err(borrowed) => todo!(),
        }
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
    tree: &TreeNodeRef<S>,
    prefix: &[u8],
    f: impl Fn(FindResult<&TreeNodeRef<S>>) -> Result<T, S::Error>,
) -> Result<T, S::Error> {
    let tree_prefix = tree.load_prefix(store)?;
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
        let tree_children = tree.load_children(store)?;
        if let Some(child) = tree_children.find(c) {
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

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

trait NodeConverter<A, B> {
    fn convert_node(&self, node: &TreeNodeRef<A>, store: &A) -> Result<OwnedTreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<A>,
        store: &A,
        n: usize,
    ) -> Result<OwnedTreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_value(&self, bv: &ValueRef<A>, store: &A) -> Result<OwnedValue<B>, A::Error>
    where
        A: BlobStore;
}

#[derive(Clone, Copy)]
struct DowncastConverter;

impl<B: BlobStore> NodeConverter<NoStore, B> for DowncastConverter {
    fn convert_node(
        &self,
        node: &TreeNodeRef<NoStore>,
        _: &NoStore,
    ) -> Result<OwnedTreeNode<B>, NoError> {
        Ok(node.downcast())
    }

    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<NoStore>,
        store: &NoStore,
        n: usize,
    ) -> Result<OwnedTreeNode<B>, NoError>
    where
        NoStore: BlobStore,
    {
        node.clone_shortened(store, n).map(|x| x.downcast())
    }

    fn convert_value(&self, bv: &ValueRef<NoStore>, _: &NoStore) -> Result<OwnedValue<B>, NoError>
    where
        NoStore: BlobStore,
    {
        Ok(bv.downcast::<B>().to_owned())
    }
}

fn cmp<A, B: BlobStore>(
    a: &InPlaceVecBuilder<'_, OwnedTreeNode<A>>,
    b: &mut TreeNodeIter<'_, B>,
) -> Option<Ordering> {
    let ap = a.source_slice().get(0).map(|x| x.first_prefix_byte());
    let bp = b.first_prefix_byte_opt();
    match (ap, bp) {
        (Some(a), Some(b)) => Some(a.cmp(&b)),
        (Some(_), None) => Some(Ordering::Less),
        (None, Some(_)) => Some(Ordering::Greater),
        (None, None) => None,
    }
}

enum TreeNodeIter<'a, S> {
    Owned(std::slice::Iter<'a, OwnedTreeNode<S>>),
    Borrowed(OwnedBlob, usize),
}

impl<'a, S: BlobStore> TreeNodeIter<'a, S> {
    fn load(id: &[u8], store: &S) -> Result<Self, S::Error> {
        Ok(if id.is_empty() {
            TreeNodeIter::Owned([].iter())
        } else {
            let data = store.read(id)?;
            TreeNodeIter::<S>::Borrowed(data, 0)
        })
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Owned(x) => x.as_slice().is_empty(),
            Self::Borrowed(slice, offset) => *offset == slice.len(),
        }
    }

    fn find(&self, prefix: u8) -> Option<TreeNodeRef<S>> {
        match self {
            Self::Owned(inner) => {
                let slice = inner.as_slice();
                if slice.len() == 256 {
                    Some(TreeNodeRef::Owned(&slice[prefix as usize]))
                } else {
                    slice
                        .binary_search_by_key(&Some(prefix), |x| x.first_prefix_byte())
                        .ok()
                        .map(|i| TreeNodeRef::Owned(&slice[i]))
                }
            }
            Self::Borrowed(x, o) => {
                let mut offset = *o;
                while let Some(node) = BorrowedTreeNode::<S>::read(&x[offset..]) {
                    match node.first_prefix_byte().cmp(&Some(prefix)) {
                        Ordering::Less => offset += node.bytes_len(),
                        Ordering::Equal => return Some(TreeNodeRef::Borrowed(node)),
                        Ordering::Greater => return None,
                    }
                }
                None
            }
        }
    }

    fn first_prefix_byte_opt(&mut self) -> Option<Option<u8>> {
        match self {
            Self::Owned(x) => x.as_slice().get(0).map(|x| x.first_prefix_byte()),
            Self::Borrowed(slice, offset) => {
                BorrowedTreeNode::<S>::read(&slice[*offset..]).map(|x| x.first_prefix_byte())
            }
        }
    }

    fn next(&mut self) -> Option<TreeNodeRef<'_, S>> {
        match self {
            Self::Owned(x) => x.next().map(TreeNodeRef::Owned),
            Self::Borrowed(slice, offset) => {
                if let Some(node) = BorrowedTreeNode::read(&slice[*offset..]) {
                    *offset += node.bytes_len();
                    Some(TreeNodeRef::Borrowed(node))
                } else {
                    None
                }
            }
        }
    }
}

/// Outer combine two trees with a function f
fn outer_combine_with<A, B, C, F>(
    a: &mut OwnedTreeNode<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    c: C,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    C: NodeConverter<B, A> + Clone,
    A::Error: From<B::Error>,
    F: Fn(&mut OwnedValue<A>, &ValueRef<B>) -> Result<(), B::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_ref().raw);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_raw(av.as_ref().raw);
            }
        }
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        outer_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        let ac = a.load_children_mut(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        outer_combine_children_with(ac, ab, TreeNodeIter::Owned(bc.iter()), bb, c, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        a.split(&ab, n)?;
        // prefixes are identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_ref().raw);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_raw(av.as_ref().raw);
            }
        }
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        outer_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else {
        // the two nodes are disjoint
        a.split(&ab, n)?;
        let ac = a.load_children_mut(&ab)?;
        ac.push(c.convert_node_shortened(b, &bb, n)?);
        ac.sort_by_key(|x| x.first_prefix_byte());
    }
    a.canonicalize();
    Ok(())
}

fn outer_combine_children_with<'a, A, B, C, F>(
    ac: &'a mut Vec<OwnedTreeNode<A>>,
    ab: A,
    mut bc: TreeNodeIter<'a, B>,
    bb: B,
    c: C,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    C: NodeConverter<B, A> + Clone,
    F: Fn(&mut OwnedValue<A>, &ValueRef<B>) -> Result<(), B::Error> + Copy,
    A::Error: From<B::Error>,
{
    if ac.is_empty() || bc.is_empty() {
        while let Some(bc) = bc.next() {
            ac.push(c.convert_node(&bc, &bb)?);
        }
        Ok(())
    } else {
        let mut acb = InPlaceVecBuilder::from(ac);
        let mut bci = bc;
        while let Some(ordering) = cmp(&acb, &mut bci) {
            match ordering {
                Ordering::Less => {
                    acb.consume(1, true);
                }
                Ordering::Equal => {
                    // the .unwrap() are safe because cmp guarantees that there is a value on both sides
                    let ac = acb.source_slice_mut().get_mut(0).unwrap();
                    let bc = bci.next().unwrap();
                    outer_combine_with(ac, ab.clone(), &bc, bb.clone(), c.clone(), f)?;
                    // only move if the child is non-empty
                    let non_empty = !ac.is_empty();
                    acb.consume(1, non_empty);
                }
                Ordering::Greater => {
                    // the .unwrap() is safe because cmp guarantees that there is a value
                    let b = bci.next().unwrap();
                    let a = c.convert_node(&b, &bb)?;
                    acb.push(a);
                }
            }
        }
        Ok(())
    }
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

    let store = MemStore2::default();
    let mut target = Vec::new();
    let t0 = Instant::now();
    // t.dump(0, &NoStore).unwrap();
    t.serialize(&mut target, &store).unwrap();
    println!("{} {}", Hex::new(&target), t0.elapsed().as_secs_f64());
    let d = OwnedTreeNode::<MemStore2>::deserialize(&target).unwrap();
    println!("{:?}", d);
    // d.dump(0, &store).unwrap();

    let t0 = Instant::now();
    for (key, value) in &elems3 {
        let v: OwnedValue<MemStore2> = d.get(&key, &store).unwrap().unwrap();
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

pub struct TreeNode<'a, S: BlobStore = NoStore> {
    prefix_len: u8,
    value_len: u8,
    children_len: u8,
    prefix: &'a u8,
    value: &'a u8,
    children: &'a u8,
    p: PhantomData<S>,
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
            prefix_len: 1,
            value_len: 1,
            children_len: 1,
            prefix: &TreePrefixRef::<S>::empty().1 .1[0],
            value: &TreeValueOptRef::<S>::none().1 .1[0],
            children: &TreeChildrenRef::<S>::empty().1 .1[0],
            p: PhantomData,
        }
    }

    pub fn prefix(&self) -> &TreePrefixRef<S> {
        let slice: &[u8] =
            unsafe { std::slice::from_raw_parts(self.prefix, self.prefix_len as usize) };
        unsafe { std::mem::transmute(slice) }
    }

    pub fn value(&self) -> &TreeValueOptRef<S> {
        let slice: &[u8] =
            unsafe { std::slice::from_raw_parts(self.value, self.value_len as usize) };
        unsafe { std::mem::transmute(slice) }
    }

    pub fn children(&self) -> &TreeChildrenRef<S> {
        let slice: &[u8] =
            unsafe { std::slice::from_raw_parts(self.children, self.children_len as usize) };
        unsafe { std::mem::transmute(slice) }
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
                prefix_len: prefix.bytes().len() as u8,
                prefix: &prefix.1 .1[0],
                value_len: value.bytes().len() as u8,
                value: &value.1 .1[0],
                children_len: children.bytes().len() as u8,
                children: &children.1 .1[0],
                p: PhantomData,
            },
            &rest,
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
    pub fn first_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.value().is_some() {
            Some(TreeValueRefWrapper::new(Blob::copy_from_slice(
                self.value().bytes(),
            )))
        } else {
            let children = self.children().load(store)?;
            children.iter().next().unwrap().first_value(store)?
        })
    }

    /// get the first entry
    pub fn first_entry(
        &self,
        store: &S,
        mut prefix: Vec<u8>,
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        prefix.extend_from_slice(&self.prefix().load(store)?);
        Ok(if self.value().is_some() {
            Some((
                Blob::copy_from_slice(&prefix),
                TreeValueRefWrapper::new(Blob::copy_from_slice(self.value().bytes())),
            ))
        } else {
            let children = self.children().load(store)?;
            children.iter().next().unwrap().first_entry(store, prefix)?
        })
    }

    /// get the last value
    pub fn last_value(&self, store: &S) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
        Ok(if self.children().is_empty() {
            self.value()
                .value_opt()
                .map(|x| TreeValueRefWrapper::new(Blob::copy_from_slice(x.bytes())))
        } else {
            let children = self.children().load(store)?;
            children.iter().last().unwrap().last_value(store)?
        })
    }

    /// get the first entry
    pub fn last_entry(
        &self,
        store: &S,
        mut prefix: Vec<u8>,
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        prefix.extend_from_slice(&self.prefix().load(store)?);
        Ok(if self.children().is_empty() {
            self.value().value_opt().map(|x| {
                (
                    Blob::copy_from_slice(&prefix),
                    TreeValueRefWrapper::new(Blob::copy_from_slice(x.bytes())),
                )
            })
        } else {
            let c = self.children().load(store)?;
            c.iter().last().unwrap().last_entry(store, prefix)?
        })
    }

    pub fn validate(&self, store: &S) -> Result<bool, S::Error> {
        if self.prefix().1.ref_count() > 100 {
            return Ok(false);
        }
        if self.value().1.ref_count() > 100 {
            return Ok(false);
        }
        for child in self.children().load(store)?.iter() {
            if !child.validate(store)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
