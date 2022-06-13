use core::borrow;
use std::{
    any::{self, Any, TypeId},
    borrow::Borrow,
    cmp::Ordering,
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    mem::ManuallyDrop,
    num::NonZeroU8,
    ops::Deref,
    process::Child,
    slice,
    sync::Arc,
    time::Instant,
};

use inplace_vec_builder::InPlaceVecBuilder;

use crate::{
    store::{unwrap_safe, Blob, BlobStore, MemStore, NoError, NoStore, OwnedBlob},
    Hex,
};
use std::fmt::Debug;
#[cfg(test)]
mod tests;

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
pub struct Raw<'a> {
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

pub struct IdOrData<'a> {
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

pub enum ValueRef<'a, S> {
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

    fn detached(&self, store: &S) -> Result<OwnedValue<NoStore>, S::Error>
    where
        S: BlobStore,
    {
        if !self.is_id() {
            Ok(unsafe { std::mem::transmute(self.to_owned()) })
        } else {
            todo!()
        }
    }

    fn read(&self) -> Result<&[u8], &[u8]> {
        if !self.is_id() {
            Ok(self.slice())
        } else {
            Err(self.slice())
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

impl AsRef<[u8]> for OwnedValue<NoStore> {
    fn as_ref(&self) -> &[u8] {
        self.data.slice(self.hdr)
    }
}

impl Deref for OwnedValue<NoStore> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data.slice(self.hdr)
    }
}

impl OwnedValue<NoStore> {
    fn downcast<S2: BlobStore>(self) -> OwnedValue<S2> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<S> OwnedValue<S> {
    const EMPTY: Self = Self {
        hdr: Header::NONE,
        data: ArcOrInlineBlob::EMPTY,
        p: PhantomData,
    };

    fn as_value_ref(&self) -> OwnedValueRef<S> {
        OwnedValueRef::new(Raw {
            hdr: self.hdr,
            data: &self.data,
        })
    }

    fn set(&mut self, value: &ValueRef<S>) {
        self.data.manual_drop(self.hdr);
        match value {
            ValueRef::Raw(raw, _) => {
                self.hdr = raw.hdr;
                self.data = raw.data.manual_clone(raw.hdr);
            }
            ValueRef::IdOrData(iod) => {
                self.hdr = iod.hdr;
                self.data = ArcOrInlineBlob::copy_from_slice(iod.slice());
            }
        }
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
    fn detached(&self, store: &S) -> Result<OwnedTreeNode<NoStore>, S::Error> {
        let mut res = self.clone();
        if self.prefix_hdr.is_id() {
            res.set_prefix_slice(self.load_prefix(store)?.as_ref());
        }
        if self.value_hdr.is_id() {
            res.set_value(self.load_value(store)?);
        }
        if self.children_hdr.is_id() && !self.children_hdr.is_none() {
            todo!()
        }
        Ok(unsafe { std::mem::transmute(res) })
    }

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
        let iter = self.load_children(store)?;
        if let Some(mut iter) = iter {
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
                let mut record_size = 0usize;
                for child in children.iter() {
                    let ofs = serialized.len();
                    child.serialize(&mut serialized, store)?;
                    let len = serialized.len() - ofs;
                    if record_size == 0 {
                        record_size = len;
                    } else if record_size != len {
                        record_size = usize::max_value()
                    }
                }
                let id = store.write(&serialized)?;
                target.push(Header::id(id.len() + 1).into());
                target.push(record_size.try_into().unwrap_or_default());
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

    fn load_children(&self, store: &S) -> Result<Option<TreeNodeIter<S>>, S::Error> {
        match self.get_children() {
            Ok(children) => Ok(TreeNodeIter::from_slice(children)),
            Err(id) => TreeNodeIter::load(id, store).map(Some),
        }
    }

    fn load_children_owned(&self, store: &S) -> Result<Option<TreeNodeIter<'static, S>>, S::Error> {
        match self.get_children() {
            Ok(children) => Ok(Some(TreeNodeIter::from_arc(children.clone()))),
            Err(id) => TreeNodeIter::load(id, store).map(Some),
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

impl<S: BlobStore> OwnedTreeNode<S> {
    fn load_prefix(&self, store: &S) -> Result<Blob<'_>, S::Error> {
        if self.prefix().is_id() {
            store.read(self.prefix().slice())
        } else {
            Ok(Blob::new(self.prefix().slice()))
        }
    }

    fn load_value(&self, store: &S) -> Result<Option<Blob<'_>>, S::Error> {
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

    fn empty_with_children(arc: Arc<Vec<OwnedTreeNode<S>>>) -> Self {
        let mut res = Self::EMPTY;
        res.set_children_arc(arc);
        res
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

    fn first_prefix_byte(&self) -> Option<u8> {
        self.prefix().slice().get(0).cloned()
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

    fn set_value(&mut self, value: Option<impl AsRef<[u8]>>) {
        if let Some(x) = value {
            self.set_value_slice(Some(x.as_ref()))
        } else {
            self.set_value_slice(None)
        }
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

    fn set_children_arc_opt(&mut self, arc_opt: Option<Arc<Vec<OwnedTreeNode<S>>>>) {
        match arc_opt {
            Some(arc) => self.set_children_arc(arc),
            None => self.set_children_id(&[]),
        }
    }

    fn set_children_id(&mut self, id: &[u8]) {
        self.children.manual_drop(self.children_hdr);
        self.children_hdr = Header::id(id.len());
        self.children = ChildrenRef::id_from_slice(id);
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
    fn detached(&self, store: &S) -> Result<OwnedTreeNode<NoStore>, S::Error>
    where
        S: BlobStore,
    {
        todo!()
    }

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
        if let Some(mut iter) = iter {
            while let Some(child) = iter.next() {
                child.dump(indent + 4, store)?;
            }
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

    fn load_children(&self, store: &S) -> Result<Option<TreeNodeIter<'static, S>>, S::Error>
    where
        S: BlobStore,
    {
        if self.children_hdr.is_none() {
            Ok(TreeNodeIter::from_slice(&[]))
        } else {
            assert!(self.children_hdr.is_id());
            let id = self.children().slice();
            TreeNodeIter::load(id, store).map(Some)
        }
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
pub enum TreeNodeRef<'a, S> {
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
    fn detached(&self, store: &S) -> Result<OwnedTreeNode<NoStore>, S::Error> {
        match self {
            Self::Owned(inner) => inner.detached(store),
            Self::Borrowed(inner) => inner.detached(store),
        }
    }

    fn dispatch(&self) -> Result<&'a OwnedTreeNode<S>, &'a BorrowedTreeNode<S>> {
        match self {
            Self::Owned(inner) => Ok(inner),
            Self::Borrowed(inner) => Err(inner),
        }
    }

    fn to_owned(&self) -> OwnedTreeNode<S> {
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

    fn load_children(&self, store: &S) -> Result<Option<TreeNodeIter<S>>, S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.load_children(store),
            Err(borrowed) => borrowed.load_children(store),
        }
    }

    fn load_children_owned(&self, store: &S) -> Result<Option<TreeNodeIter<'static, S>>, S::Error> {
        match self {
            Self::Owned(owned) => owned.load_children_owned(store),
            Self::Borrowed(borrowed) => borrowed.load_children(store),
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
    f: impl FnOnce(FindResult<&TreeNodeRef<S>>) -> Result<T, S::Error>,
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
        if let Some(tree_children) = tree_children {
            if let Some(child) = tree_children.find(c) {
                return find(store, &child, &prefix[n..], f);
            } else {
                FindResult::NotFound
            }
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

pub trait NodeConverter<A, B> {
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
    ) -> Result<OwnedTreeNode<B>, NoError> {
        node.clone_shortened(store, n).map(|x| x.downcast())
    }

    fn convert_value(&self, bv: &ValueRef<NoStore>, _: &NoStore) -> Result<OwnedValue<B>, NoError> {
        Ok(bv.downcast::<B>().to_owned())
    }
}

/// A converter that converts from one store to another store by just completely detaching it
#[derive(Clone, Copy)]
pub struct DetachConverter;

impl<A: BlobStore, B: BlobStore> NodeConverter<A, B> for DetachConverter {
    fn convert_node(&self, node: &TreeNodeRef<A>, store: &A) -> Result<OwnedTreeNode<B>, A::Error> {
        Ok(node.detached(store)?.downcast())
    }
    fn convert_value(&self, value: &ValueRef<A>, store: &A) -> Result<OwnedValue<B>, A::Error> {
        Ok(value.detached(store)?.downcast())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<A>,
        store: &A,
        n: usize,
    ) -> Result<OwnedTreeNode<B>, A::Error> {
        let node = node.clone_shortened(store, n)?;
        Ok(self.convert_node(&node.as_ref(), store)?.downcast())
    }
}

struct OuterJoin<'a, A: BlobStore, B: BlobStore, E> {
    a: TreeNodeIter<'a, A>,
    b: TreeNodeIter<'a, B>,
    p: PhantomData<E>,
}

impl<'a, A, B, E> OuterJoin<'a, A, B, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error>,
{
    pub fn new(a: TreeNodeIter<'a, A>, b: TreeNodeIter<'a, B>) -> Self {
        Self {
            a,
            b,
            p: PhantomData,
        }
    }

    // get the next a, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_a(&mut self) -> Option<(Option<TreeNodeRef<'_, A>>, Option<TreeNodeRef<'_, B>>)> {
        let a = self.a.next().unwrap();
        Some((Some(a), None))
    }

    // get the next b, assuming we know already that it is not an Err
    #[allow(clippy::type_complexity)]
    fn next_b(&mut self) -> Option<(Option<TreeNodeRef<'_, A>>, Option<TreeNodeRef<'_, B>>)> {
        let b = self.b.next().unwrap();
        Some((None, Some(b)))
    }

    // get the next a and b, assuming we know already that neither is an Err
    #[allow(clippy::type_complexity)]
    fn next_ab(&mut self) -> Option<(Option<TreeNodeRef<'_, A>>, Option<TreeNodeRef<'_, B>>)> {
        let a = self.a.next();
        let b = self.b.next();
        Some((a, b))
    }

    fn next0(
        &mut self,
    ) -> Result<Option<(Option<TreeNodeRef<'_, A>>, Option<TreeNodeRef<'_, B>>)>, E> {
        Ok(
            if let (Some(ak), Some(bk)) = (
                self.a.first_prefix_byte_opt(),
                self.b.first_prefix_byte_opt(),
            ) {
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

    fn next(
        &mut self,
    ) -> Option<Result<(Option<TreeNodeRef<'_, A>>, Option<TreeNodeRef<'_, B>>), E>> {
        self.next0().transpose()
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

unsafe fn extend_lifetime<T>(value: &T) -> &'static T {
    std::mem::transmute(value)
}

struct OwnedTreeNodeIter<'a, S>(
    Option<Arc<Vec<OwnedTreeNode<S>>>>,
    slice::Iter<'a, OwnedTreeNode<S>>,
);

impl<S: BlobStore> OwnedTreeNodeIter<'static, S> {
    fn new_owned(owner: Arc<Vec<OwnedTreeNode<S>>>) -> Self {
        let iter = unsafe { extend_lifetime(owner.as_ref()) }.iter();
        Self(Some(owner.clone()), iter)
    }
}

fn is_no_store<S: 'static>() -> bool {
    TypeId::of::<S>() == TypeId::of::<NoStore>()
}

impl<'a, S: BlobStore> OwnedTreeNodeIter<'a, S> {
    fn new(slice: &'a [OwnedTreeNode<S>]) -> Self {
        Self(None, slice.iter())
    }

    fn to_owned(mut self) -> Option<Arc<Vec<OwnedTreeNode<S>>>> {
        if let Some(arc) = self.0.as_ref() {
            Some(arc.clone())
        } else if self.is_empty() {
            None
        } else {
            let mut res = Vec::with_capacity(self.1.len());
            while let Some(x) = self.next() {
                res.push(x.to_owned());
            }
            Some(Arc::new(res))
        }
    }

    fn detached(mut self, store: &S) -> Result<Option<Arc<Vec<OwnedTreeNode<NoStore>>>>, S::Error> {
        Ok(if self.is_empty() {
            None
        } else if self.0.is_some() && is_no_store::<S>() {
            unsafe { std::mem::transmute(self.0.clone()) }
        } else {
            let mut res = Vec::with_capacity(self.1.len());
            while let Some(x) = self.next() {
                res.push(x.detached(store)?);
            }
            Some(Arc::new(res))
        })
    }

    fn is_empty(&self) -> bool {
        self.1.as_slice().is_empty()
    }

    fn find(&self, prefix: u8) -> Option<&OwnedTreeNode<S>> {
        let slice = self.1.as_slice();
        if slice.len() == 256 {
            Some(&slice[prefix as usize])
        } else {
            slice
                .binary_search_by_key(&Some(prefix), |x| x.first_prefix_byte())
                .ok()
                .map(|i| &slice[i])
        }
    }

    fn first_prefix_byte_opt(&mut self) -> Option<Option<u8>> {
        self.1.as_slice().get(0).map(|x| x.first_prefix_byte())
    }

    fn next(&mut self) -> Option<&OwnedTreeNode<S>> {
        self.1.next()
    }
}

struct BorrowedTreeNodeIter<S> {
    data: OwnedBlob,
    offset: usize,
    record_size: u8,
    p: PhantomData<S>,
}

impl<S: BlobStore> BorrowedTreeNodeIter<S> {
    fn load(id: &[u8], store: &S) -> Result<Self, S::Error> {
        let (data, record_size) = if id.is_empty() {
            (Blob::empty(), 0)
        } else {
            (store.read(&id[1..])?, id[0])
        };
        Ok(Self {
            data,
            offset: 0,
            record_size,
            p: PhantomData,
        })
    }

    fn to_owned(self) -> Option<Arc<Vec<OwnedTreeNode<S>>>> {
        if self.is_empty() {
            None
        } else {
            todo!()
        }
    }

    fn detached(self, store: &S) -> Result<Option<Arc<Vec<OwnedTreeNode<NoStore>>>>, S::Error> {
        Ok(if self.is_empty() { None } else { todo!() })
    }

    fn is_empty(&self) -> bool {
        self.offset == self.data.len()
    }

    fn find(&self, prefix: u8) -> Option<BorrowedTreeNode<S>> {
        let elems = if self.record_size != 0 {
            (self.data.len() - self.offset) / (self.record_size as usize)
        } else {
            0
        };
        let mut offset = self.offset;
        while let Some(node) = BorrowedTreeNode::<S>::read(&self.data[offset..]) {
            match node.first_prefix_byte().cmp(&Some(prefix)) {
                Ordering::Less => offset += node.bytes_len(),
                Ordering::Equal => return Some(node),
                Ordering::Greater => return None,
            }
        }
        None
    }

    fn first_prefix_byte_opt(&mut self) -> Option<Option<u8>> {
        BorrowedTreeNode::<S>::read(&self.data[self.offset..]).map(|x| x.first_prefix_byte())
    }

    fn next(&mut self) -> Option<BorrowedTreeNode<S>> {
        if let Some(node) = BorrowedTreeNode::read(&self.data[self.offset..]) {
            self.offset += node.bytes_len();
            Some(node)
        } else {
            None
        }
    }
}

enum TreeNodeIter<'a, S> {
    Owned(OwnedTreeNodeIter<'a, S>),
    Borrowed(BorrowedTreeNodeIter<S>),
}

impl<S: BlobStore> TreeNodeIter<'static, S> {
    fn from_arc(arc: Arc<Vec<OwnedTreeNode<S>>>) -> Self {
        Self::Owned(OwnedTreeNodeIter::new_owned(arc))
    }
}

impl<'a, S: BlobStore> TreeNodeIter<'a, S> {
    fn load(id: &[u8], store: &S) -> Result<Self, S::Error> {
        Ok(Self::Borrowed(BorrowedTreeNodeIter::load(id, store)?))
    }

    fn from_slice(slice: &'a [OwnedTreeNode<S>]) -> Option<Self> {
        if !slice.is_empty() {
            Some(Self::Owned(OwnedTreeNodeIter::new(slice)))
        } else {
            None
        }
    }

    fn to_owned(self) -> Option<Arc<Vec<OwnedTreeNode<S>>>> {
        match self {
            Self::Owned(x) => x.to_owned(),
            Self::Borrowed(x) => x.to_owned(),
        }
    }

    fn detached(self, store: &S) -> Result<Option<Arc<Vec<OwnedTreeNode<NoStore>>>>, S::Error> {
        match self {
            Self::Owned(x) => x.detached(store),
            Self::Borrowed(x) => x.detached(store),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Owned(x) => x.is_empty(),
            Self::Borrowed(x) => x.is_empty(),
        }
    }

    fn find(&self, prefix: u8) -> Option<TreeNodeRef<S>> {
        match self {
            Self::Owned(x) => x.find(prefix).map(|x| TreeNodeRef::Owned(x)),
            Self::Borrowed(x) => x.find(prefix).map(|x| TreeNodeRef::Borrowed(x)),
        }
    }

    fn first_prefix_byte_opt(&mut self) -> Option<Option<u8>> {
        match self {
            Self::Owned(x) => x.first_prefix_byte_opt(),
            Self::Borrowed(x) => x.first_prefix_byte_opt(),
        }
    }

    fn next(&mut self) -> Option<TreeNodeRef<'_, S>> {
        match self {
            Self::Owned(x) => x.next().map(|x| TreeNodeRef::Owned(x)),
            Self::Borrowed(x) => x.next().map(|x| TreeNodeRef::Borrowed(x)),
        }
    }
}

fn scan_prefix<S: BlobStore + Clone>(
    store: S,
    tree: &TreeNodeRef<S>,
    prefix: &[u8],
) -> Result<Iter<S>, S::Error> {
    let store1 = store.clone();
    find(&store, tree, prefix, |r| {
        Ok(match r {
            FindResult::Found(tree) => {
                let prefix = IterKey::new(prefix);
                let tree: OwnedTreeNode<S> = tree.to_owned();
                Iter::new(TreeNodeIter::from_arc(Arc::new(vec![tree])), store1, prefix)
            }
            FindResult::Prefix { tree, matching } => {
                let prefix = IterKey::new(&prefix[..prefix.len() - matching]);
                let tree: OwnedTreeNode<S> = tree.to_owned();
                Iter::new(TreeNodeIter::from_arc(Arc::new(vec![tree])), store1, prefix)
            }
            FindResult::NotFound => Iter::empty(store1),
        })
    })
}

/// Outer combine two trees with a function f
fn outer_combine<A, B, E, F>(
    a: &TreeNodeRef<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<OwnedTreeNode<NoStore>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<OwnedValue<NoStore>>, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let mut res = OwnedTreeNode::EMPTY;
    res.set_prefix_slice(&ap[..n]);
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        res.set_value(if let Some(bv) = b.value_opt() {
            if let Some(av) = a.value_opt() {
                f(&av, &bv)?
            } else {
                Some(bv.detached(&bb)?)
            }
        } else {
            a.value_opt().map(|x| x.detached(&ab)).transpose()?
        });
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        res.set_children_arc_opt(outer_combine_children(ac, ab, bc, bb, f)?);
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        res.set_value(a.value_opt().map(|x| x.detached(&ab)).transpose()?);
        let ac = a.load_children(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        res.set_children_arc_opt(outer_combine_children(
            ac,
            ab,
            TreeNodeIter::from_slice(&bc),
            bb,
            f,
        )?);
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        res.set_value(b.value_opt().map(|x| x.detached(&bb)).transpose()?);
        let ac = [a.clone_shortened(&ab, n)?];
        let bc = b.load_children(&bb)?;
        res.set_children_arc_opt(outer_combine_children(
            TreeNodeIter::from_slice(&ac),
            ab,
            bc,
            bb,
            f,
        )?);
    } else {
        // the two nodes are disjoint
        // value is none
        res.set_value_slice(None);
        // children is just the shortened children a and b in the right order
        let mut a: OwnedTreeNode<NoStore> = DetachConverter.convert_node_shortened(&a, &ab, n)?;
        let mut b: OwnedTreeNode<NoStore> = DetachConverter.convert_node_shortened(&b, &bb, n)?;
        if ap[n] > bp[n] {
            std::mem::swap(&mut a, &mut b);
        }
        res.set_children_arc(Arc::new(vec![a, b]));
    }
    res.canonicalize();
    Ok(res)
}

fn outer_combine_children<'a, A, B, E, F>(
    ac: Option<TreeNodeIter<'a, A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<Option<Arc<Vec<OwnedTreeNode<NoStore>>>>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<OwnedValue<NoStore>>, E> + Copy,
{
    Ok(match (ac, bc) {
        (Some(ac), Some(bc)) => {
            let mut res = Vec::new();
            let mut iter = OuterJoin::<A, B, E>::new(ac, bc);
            while let Some(x) = iter.next() {
                let r = match x? {
                    (Some(a), Some(b)) => outer_combine(&a, ab.clone(), &b, bb.clone(), f)?,
                    (Some(a), None) => a.detached(&ab)?,
                    (None, Some(b)) => b.detached(&bb)?,
                    (None, None) => panic!(),
                };
                res.push(r);
            }
            Some(Arc::new(res))
        }
        (None, Some(bc)) => bc.detached(&bb)?,
        (Some(ac), None) => ac.detached(&ab)?,
        (None, None) => None,
    })
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
    F: Fn(&mut OwnedValue<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_value_ref().raw);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_raw(av.as_value_ref().raw);
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
        outer_combine_children_with(ac, ab, TreeNodeIter::from_slice(&bc), bb, c, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        a.split(&ab, n)?;
        // prefixes are identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_raw(av.as_value_ref().raw);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_raw(av.as_value_ref().raw);
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
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    c: C,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    C: NodeConverter<B, A> + Clone,
    F: Fn(&mut OwnedValue<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
    A::Error: From<B::Error>,
{
    if let Some(mut bc) = bc {
        if ac.is_empty() {
            while let Some(bc) = bc.next() {
                ac.push(c.convert_node(&bc, &bb)?);
            }
            return Ok(());
        }
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
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Tree<S: BlobStore = NoStore> {
    node: OwnedTreeNode<S>,
    /// The associated store
    store: S,
}

impl<S: BlobStore + Default> Default for Tree<S> {
    fn default() -> Self {
        Self::empty(S::default())
    }
}

impl Tree {
    pub fn leaf(value: &[u8]) -> Self {
        Self::new(OwnedTreeNode::leaf(value), NoStore)
    }

    pub fn single(key: &[u8], value: &[u8]) -> Self {
        Self::new(OwnedTreeNode::single(key, value), NoStore)
    }

    pub fn get(&self, key: &[u8]) -> Option<OwnedValue<NoStore>> {
        unwrap_safe(self.try_get(key))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        unwrap_safe(self.try_contains_key(key))
    }

    pub fn iter(&self) -> impl Iterator<Item = (IterKey, OwnedValue<NoStore>)> {
        self.try_iter().map(unwrap_safe)
    }

    pub fn values(&self) -> impl Iterator<Item = OwnedValue<NoStore>> {
        self.try_values().map(unwrap_safe)
    }

    pub fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> impl Iterator<Item = (IterKey, OwnedValue<NoStore>)> + '_ {
        unwrap_safe(self.try_scan_prefix(prefix)).map(unwrap_safe)
    }

    pub fn dump(&self) {
        unwrap_safe(self.try_dump())
    }

    pub fn outer_combine(
        &self,
        that: &Tree,
        f: impl Fn(&ValueRef<NoStore>, &ValueRef<NoStore>) -> Option<OwnedValue<NoStore>> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_outer_combine(that, |a, b| Ok(f(a, b))))
    }

    pub fn outer_combine_with(
        &mut self,
        that: &Tree,
        f: impl Fn(&mut OwnedValue<NoStore>, &ValueRef<NoStore>) + Copy,
    ) {
        unwrap_safe(self.try_outer_combine_with(that, DowncastConverter, |a, b| Ok(f(a, b))))
    }
}

#[derive(Debug, Clone, Default)]
pub struct IterKey(Arc<Vec<u8>>);

impl IterKey {
    fn new(root: &[u8]) -> Self {
        Self(Arc::new(root.to_vec()))
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

impl AsRef<[u8]> for IterKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Borrow<[u8]> for IterKey {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for IterKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub struct Iter<S: BlobStore> {
    path: IterKey,
    stack: Vec<(
        usize,
        Option<TreeNodeIter<'static, S>>,
    )>,
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

    fn new(iter: TreeNodeIter<'static, S>, store: S, prefix: IterKey) -> Self {
        Self {
            stack: vec![(0, Some(iter))],
            path: prefix,
            store,
        }
    }

    fn next0(&mut self) -> Result<Option<(IterKey, OwnedValue<S>)>, S::Error> {
        while !self.stack.is_empty() {
            let (last_prefix_len, iter_opt) = &mut self.stack.last_mut().unwrap();
            let last_prefix_len = *last_prefix_len;
            if let Some(iter) = iter_opt {
                if let Some(node) = iter.next() {
                    let value = node.value_opt().map(|x| x.to_owned());
                    let prefix = node.load_prefix(&self.store)?;
                    let prefix_len = prefix.len();
                    let children = node.load_children_owned(&self.store)?;
                    self.path.append(prefix.as_ref());
                    self.stack.push((prefix_len, children));
                    if let Some(value) = value {
                        return Ok(Some((self.path.clone(), value)));
                    }
                } else {
                    *iter_opt = None;
                }
            } else {
                self.path.pop(last_prefix_len);
                self.stack.pop();
            }
        }
        Ok(None)
    }
}

impl<S: BlobStore> Iterator for Iter<S> {
    type Item = Result<(IterKey, OwnedValue<S>), S::Error>;

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
    stack: Vec<TreeNodeIter<'static, S>>,
    store: S,
}

impl<S: BlobStore> Values<S> {
    fn empty(store: S) -> Self {
        Self {
            stack: Vec::new(),
            store,
        }
    }

    fn new(iter: TreeNodeIter<'static, S>, store: S) -> Self {
        Self {
            stack: vec![iter],
            store,
        }
    }

    fn next0(&mut self) -> Result<Option<OwnedValue<S>>, S::Error> {
        while !self.stack.is_empty() {
            if let Some(node) = self.stack.last_mut().unwrap().next() {
                let value = node.value_opt().map(|x| x.to_owned());
                if let Some(children) = node.load_children_owned(&self.store)? {
                    self.stack.push(children);
                }
                if let Some(value) = value {
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
    type Item = Result<OwnedValue<S>, S::Error>;

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

impl<S: BlobStore> Tree<S> {
    pub fn empty(store: S) -> Self {
        Self::new(OwnedTreeNode::<S>::EMPTY, store)
    }

    fn new(node: OwnedTreeNode<S>, store: S) -> Self {
        Self { node, store }
    }
}

impl FromIterator<(Vec<u8>, Vec<u8>)> for Tree {
    fn from_iter<T: IntoIterator<Item = (Vec<u8>, Vec<u8>)>>(iter: T) -> Self {
        let mut tree = Tree::default();
        for (k, v) in iter.into_iter() {
            tree.outer_combine_with(&Tree::single(k.as_ref(), v.as_ref()), |_, _| {});
        }
        tree
    }
}
impl<S: BlobStore + Clone> Tree<S> {
    pub(crate) fn try_dump(&self) -> Result<(), S::Error> {
        self.node.dump(0, &self.store)
    }

    /// Get the value for a given key
    pub fn try_get(&self, key: &[u8]) -> Result<Option<OwnedValue<S>>, S::Error> {
        self.node.get(key, &self.store)
    }

    /// True if key is contained in this set
    pub fn try_contains_key(&self, key: &[u8]) -> Result<bool, S::Error> {
        self.node.contains_key(key, &self.store)
    }

    pub fn try_iter(&self) -> Iter<S> {
        Iter::new(
            TreeNodeIter::from_arc(Arc::new(vec![self.node.clone()])),
            self.store.clone(),
            IterKey::default(),
        )
    }

    pub fn try_values(&self) -> Values<S> {
        Values::new(
            TreeNodeIter::from_arc(Arc::new(vec![self.node.clone()])),
            self.store.clone(),
        )
    }
    pub fn try_scan_prefix(&self, prefix: &[u8]) -> Result<Iter<S>, S::Error> {
        scan_prefix(self.store.clone(), &TreeNodeRef::Owned(&self.node), prefix)
    }

    pub fn try_outer_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore + Clone,
        E: From<S2::Error> + From<S::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<Option<OwnedValue<NoStore>>, E> + Copy,
    {
        Ok(Tree {
            node: outer_combine(
                &TreeNodeRef::Owned(&self.node),
                self.store.clone(),
                &TreeNodeRef::Owned(&that.node),
                that.store.clone(),
                f,
            )?,
            store: NoStore,
        })
    }

    pub fn try_outer_combine_with<S2, C, F>(
        &mut self,
        that: &Tree<S2>,
        c: C,
        f: F,
    ) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        C: NodeConverter<S2, S> + Clone,
        F: Fn(&mut OwnedValue<S>, &ValueRef<S2>) -> Result<(), S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        outer_combine_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::Owned(&that.node),
            that.store.clone(),
            c,
            f,
        )
    }
}
