use std::{cmp::Ordering, fmt, marker::PhantomData, mem::ManuallyDrop, ops::Deref, sync::Arc};

use inplace_vec_builder::InPlaceVecBuilder;

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

union ArcOrInline {
    arc: ManuallyDrop<Arc<Vec<u8>>>,
    inline: [u8; PTR_SIZE],
}

impl ArcOrInline {
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

    fn slice(&self, len: usize) -> &[u8] {
        unsafe {
            if len <= PTR_SIZE {
                &self.inline[..len]
            } else {
                self.arc.as_ref().as_ref()
            }
        }
    }

    fn manual_clone(&self, len: usize) -> Self {
        unsafe {
            if len > PTR_SIZE {
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

    fn manual_drop(&mut self, len: usize) {
        unsafe {
            if len > PTR_SIZE {
                ManuallyDrop::drop(&mut self.arc);
                // just to be on the safe side, dropping twice will be like dropping a null ptr
                self.inline = [0; PTR_SIZE];
            }
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
    const EMPTY: Header = Header(0x00);
    const NONE: Header = Header(0x80);

    const fn id(len: usize) -> Self {
        debug_assert!(len < 0x80);
        Self((len as u8) | 0x80)
    }

    const fn inline(len: usize) -> Self {
        debug_assert!(len < 0x80);
        Self(len as u8)
    }

    const fn data(len: usize) -> Self {
        if len <= PTR_SIZE {
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
    data: &'a ArcOrInline,
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
        data: &ArcOrInline::EMPTY,
    };

    fn new(hdr: Header, data: &'a ArcOrInline) -> Self {
        Self { hdr, data }
    }

    fn len(&self) -> usize {
        self.hdr.len()
    }

    fn is_id(&self) -> bool {
        self.hdr.is_id()
    }

    fn is_none(&self) -> bool {
        self.hdr.is_none()
    }

    fn slice(&self) -> &'a [u8] {
        self.data.slice(self.len())
    }
}

struct IdOrInline<'a> {
    hdr: Header,
    data: &'a u8,
}

impl<'a> IdOrInline<'a> {
    fn new(hdr: Header, data: &'a u8) -> Self {
        Self { hdr, data }
    }

    fn is_id(&self) -> bool {
        self.hdr.is_id()
    }

    fn is_none(&self) -> bool {
        self.hdr.is_none()
    }

    fn slice(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.hdr.len()) }
    }
}

enum IdOrInlineOrRaw<'a> {
    Raw(Raw<'a>),
    IdOrInline(IdOrInline<'a>),
}

impl<'a> IdOrInlineOrRaw<'a> {
    fn is_id(&self) -> bool {
        match self {
            Self::Raw(x) => x.is_id(),
            Self::IdOrInline(x) => x.is_id(),
        }
    }
    fn is_none(&self) -> bool {
        match self {
            Self::Raw(x) => x.is_none(),
            Self::IdOrInline(x) => x.is_none(),
        }
    }
    fn slice(&self) -> &'a [u8] {
        match self {
            Self::Raw(x) => x.slice(),
            Self::IdOrInline(x) => x.slice(),
        }
    }
}

pub struct ValueRef<'a, S>(Raw<'a>, PhantomData<S>);

pub struct Value<S> {
    hdr: Header,
    data: ArcOrInline,
    p: PhantomData<S>,
}

impl<S> Value<S> {
    const EMPTY: Self = Self {
        hdr: Header::NONE,
        data: ArcOrInline::EMPTY,
        p: PhantomData,
    };

    fn as_ref(&self) -> ValueRef<S> {
        ValueRef(
            Raw {
                hdr: self.hdr,
                data: &self.data,
            },
            PhantomData,
        )
    }
}

#[repr(C)]
pub struct OwnedTreeNode<S> {
    dummy: u8,
    prefix_hdr: Header,
    value_hdr: Header,
    prefix: ArcOrInline,
    value: ArcOrInline,
    children: Option<Arc<Vec<OwnedTreeNode<S>>>>,
    p: PhantomData<S>,
}

impl<S> Debug for OwnedTreeNode<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedTreeNode")
            .field("prefix", &self.prefix())
            .field("value", &self.value())
            .field("children", &self.children)
            .finish()
    }
}

impl<S: BlobStore> OwnedTreeNode<S> {
    fn load_children(&self, store: &S) -> &[OwnedTreeNode<S>] {
        self.children
            .as_ref()
            .map(|x| x.as_ref().as_ref())
            .unwrap_or(&[])
    }

    fn load_children_mut(&mut self, store: &S) -> &mut Vec<OwnedTreeNode<S>> {
        let t = self.children.get_or_insert_with(|| Arc::new(Vec::new()));
        Arc::make_mut(t)
    }

    fn clone_shortened(&self, store: &S, n: usize) -> Result<Self, S::Error> {
        let prefix = self.load_prefix(store)?;
        let mut res = self.clone();
        res.set_prefix_slice(&prefix[0..n]);
        Ok(res)
    }

    fn split(&mut self, store: &S, n: usize) -> Result<(), S::Error> {
        // todo: get rid of the to_owned!
        let prefix = self.load_prefix(store)?.to_owned();
        let mut child = Self::EMPTY;
        child.set_prefix_slice(&prefix[n..]);
        self.set_prefix_slice(&prefix[..n]);
        // child.childen = self.children, self.children = empty
        std::mem::swap(&mut self.children, &mut child.children);
        // child.value = self.value, self.value = none
        std::mem::swap(&mut self.value, &mut child.value);
        std::mem::swap(&mut self.value_hdr, &mut child.value_hdr);
        // now, self is a degenerate empty node with first being the prefix
        // child is the former self (value and children) with rest as prefix
        self.children = Some(Arc::new(vec![child]));
        Ok(())
    }

    fn canonicalize(&mut self) {
        if let Some(children) = self.children.as_ref() {
            if children.is_empty() {
                self.children = None;
            }
        }
        if !self.has_value() && self.children.is_none() {
            self.set_prefix_raw(Raw::EMPTY);
        }
    }
}

impl<S> OwnedTreeNode<S> {
    const EMPTY: Self = Self {
        dummy: 0,
        prefix_hdr: Header::EMPTY,
        prefix: ArcOrInline::EMPTY,
        value_hdr: Header::NONE,
        value: ArcOrInline::EMPTY,
        children: None,
        p: PhantomData,
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
        // safe children is empty that also returns true if we have a non-canonical empty children array
        let children_is_empty = self.children.as_ref().map(|x| x.is_empty()).unwrap_or(true);
        !self.has_value() && children_is_empty
    }

    fn set_prefix_raw(&mut self, prefix: Raw) {
        self.prefix_hdr = prefix.hdr;
        self.prefix = prefix.data.manual_clone(prefix.hdr.len());
    }

    fn set_value_raw(&mut self, value: Raw) {
        self.value_hdr = value.hdr;
        self.value = value.data.manual_clone(value.hdr.len());
    }

    fn set_prefix_id_or_inline(&mut self, prefix: IdOrInline) {
        self.prefix_hdr = prefix.hdr;
        self.prefix = ArcOrInline::copy_from_slice(prefix.slice());
    }

    fn set_value_id_or_inline(&mut self, value: IdOrInline) {
        self.value_hdr = value.hdr;
        self.value = ArcOrInline::copy_from_slice(value.slice());
    }

    fn set_prefix_slice(&mut self, prefix: &[u8]) {
        self.prefix_hdr = Header::data(prefix.len());
        self.prefix = ArcOrInline::copy_from_slice(prefix);
    }

    fn set_value_slice(&mut self, value: Option<&[u8]>) {
        if let Some(value) = value {
            self.value_hdr = Header::data(value.len());
            self.value = ArcOrInline::copy_from_slice(value);
        } else {
            self.value_hdr = Header::NONE;
            self.value = ArcOrInline::EMPTY;
        }
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

    fn value_opt(&self) -> Option<ValueRef<'_, S>> {
        if self.has_value() {
            Some(ValueRef(self.value(), PhantomData))
        } else {
            None
        }
    }

    fn take_value_opt(&mut self) -> Option<Value<S>> {
        if self.has_value() {
            let mut value = Value::EMPTY;
            std::mem::swap(&mut self.value_hdr, &mut value.hdr);
            std::mem::swap(&mut self.value, &mut value.data);
            Some(value)
        } else {
            None
        }
    }

    fn is_leaf(&self) -> bool {
        self.children.is_none()
    }
}

impl<S> Drop for OwnedTreeNode<S> {
    fn drop(&mut self) {
        self.prefix.manual_drop(self.prefix_hdr.len());
        self.value.manual_drop(self.value_hdr.len());
        // just to be on the safe side
        self.value_hdr = Header::NONE;
        self.prefix_hdr = Header::EMPTY;
        self.children = None;
    }
}

impl<S> Clone for OwnedTreeNode<S> {
    fn clone(&self) -> Self {
        Self {
            dummy: 0,
            prefix_hdr: self.prefix_hdr,
            value_hdr: self.value_hdr,
            prefix: self.prefix.manual_clone(self.prefix_hdr.len()),
            value: self.value.manual_clone(self.value_hdr.len()),
            children: self.children.clone(),
            p: PhantomData,
        }
    }
}

#[repr(C)]
pub struct BorrowedTreeNode<'a, S> {
    dummy: u8,
    prefix_hdr: Header,
    value_hdr: Header,
    children_hdr: Header,
    prefix: &'a u8,
    value: &'a u8,
    children: &'a u8,
    p: PhantomData<&'a S>,
}

const EMPTY_BYTES: &'static [u8] = &[Header::EMPTY.0, Header::NONE.0, Header::NONE.0];

impl<S: 'static> BorrowedTreeNode<'static, S> {
    const EMPTY: Self = Self {
        dummy: 1,
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
    fn prefix(&self) -> IdOrInline<'a> {
        IdOrInline::new(self.prefix_hdr, self.prefix)
    }

    fn value(&self) -> IdOrInline<'a> {
        IdOrInline::new(self.value_hdr, self.value)
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
                dummy: 1,
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

pub enum TreeNode2<'a, S: BlobStore = NoStore> {
    Borrowed(BorrowedTreeNode<'a, S>),
    Owned(OwnedTreeNode<S>),
}

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

trait NodeConverter<A, B> {
    fn convert_node(
        &self,
        node: &OwnedTreeNode<A>,
        store: &A,
    ) -> Result<OwnedTreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_node_shortened(
        &self,
        node: &OwnedTreeNode<A>,
        store: &A,
        n: usize,
    ) -> Result<OwnedTreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_value(&self, bv: &ValueRef<A>, store: &A) -> Result<Value<B>, A::Error>
    where
        A: BlobStore;
}

#[derive(Clone, Copy)]
struct IdentityConverter;

impl NodeConverter<NoStore, NoStore> for IdentityConverter {
    fn convert_node(
        &self,
        node: &OwnedTreeNode<NoStore>,
        store: &NoStore,
    ) -> Result<OwnedTreeNode<NoStore>, NoError> {
        Ok(node.clone())
    }

    fn convert_node_shortened(
        &self,
        node: &OwnedTreeNode<NoStore>,
        store: &NoStore,
        n: usize,
    ) -> Result<OwnedTreeNode<NoStore>, NoError>
    where
        NoStore: BlobStore {
        node.clone_shortened(store, n)
    }

    fn convert_value(&self, bv: &ValueRef<NoStore>, store: &NoStore) -> Result<Value<NoStore>, NoError>
    where
        NoStore: BlobStore {
        todo!()
    }
}

fn cmp<A, B>(
    a: &InPlaceVecBuilder<'_, OwnedTreeNode<A>>,
    b: &std::slice::Iter<OwnedTreeNode<B>>,
) -> Option<Ordering> {
    match (a.source_slice().get(0), b.as_slice().get(0)) {
        (Some(a), Some(b)) => Some(a.first_prefix_byte().cmp(&b.first_prefix_byte())),
        (Some(_), None) => Some(Ordering::Less),
        (None, Some(_)) => Some(Ordering::Greater),
        (None, None) => None,
    }
}

/// Outer combine two trees with a function f
fn outer_combine_with<A, B, C, F>(
    a: &mut OwnedTreeNode<A>,
    ab: A,
    b: &OwnedTreeNode<B>,
    bb: B,
    c: C,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    C: NodeConverter<B, A> + Clone,
    A::Error: From<B::Error>,
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), B::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        match (a.take_value_opt(), b.value_opt()) {
            (Some(mut av), Some(bv)) => {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_ref().0);
            }
            (None, Some(bv)) => {
                // todo: convert value!
                let bv = bv;
                a.set_value_raw(bv.0);
            }
            (_, None) => {}
        }
        let ac = a.load_children_mut(&ab);
        let bc = b.load_children(&bb);
        outer_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        let ac = a.load_children_mut(&ab);
        let bc = &[b.clone_shortened(&bb, n)?];
        outer_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        a.split(&ab, n)?;
        // prefixes are identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_ref().0);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_raw(av.as_ref().0);
            }
        }
        let ac = a.load_children_mut(&ab);
        let bc = b.load_children(&bb);
        outer_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else {
        // the two nodes are disjoint
        a.split(&ab, n)?;
        let ac = a.load_children_mut(&ab);
        ac.push(c.convert_node_shortened(b, &bb, n)?);
        ac.sort_by_key(|x| x.first_prefix_byte());
    }

    a.canonicalize();
    Ok(())
}

fn outer_combine_children_with<'a, A, B, C, F>(
    ac: &'a mut Vec<OwnedTreeNode<A>>,
    ab: A,
    bc: &'a [OwnedTreeNode<B>],
    bb: B,
    c: C,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    C: NodeConverter<B, A> + Clone,
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), B::Error> + Copy,
    A::Error: From<B::Error>,
{
    if ac.is_empty() || bc.is_empty() {
        for bc in bc {
            ac.push(c.convert_node(bc, &bb)?);
        }
        Ok(())
    } else {
        let mut acb = InPlaceVecBuilder::from(ac);
        let mut bci = bc.into_iter();
        while let Some(ordering) = cmp(&acb, &bci) {
            match ordering {
                Ordering::Less => {
                    acb.consume(1, true);
                }
                Ordering::Equal => {
                    // the .unwrap() are safe because cmp guarantees that there is a value on both sides
                    let ac = acb.source_slice_mut().get_mut(0).unwrap();
                    let bc = bci.next().unwrap();
                    outer_combine_with(ac, ab.clone(), bc, bb.clone(), c.clone(), f)?;
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
    println!("{}", std::mem::size_of::<OwnedTreeNode<NoStore>>());
    println!("{}", std::mem::size_of::<BorrowedTreeNode<NoStore>>());
    println!("{}", std::mem::size_of::<TreeNode2<NoStore>>());
}

#[test]
fn new_smoke() {
    let mut a = OwnedTreeNode::<NoStore>::single(b"a", b"1");
    let mut b = OwnedTreeNode::<NoStore>::single(b"b", b"2");
    println!("{:?}", a);
    println!("{:?}", b);
    outer_combine_with(&mut a, NoStore, &b, NoStore, IdentityConverter, f).unwrap();
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
