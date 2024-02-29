//! Implementation of the radix tree node and associated plumbing
//!
//!
#![allow(dead_code, clippy::type_complexity, clippy::unit_arg)]
use std::{
    any::TypeId, borrow::Borrow, cmp::Ordering, fmt, marker::PhantomData, mem::ManuallyDrop,
    ops::Deref, slice, sync::Arc,
};

use inplace_vec_builder::InPlaceVecBuilder;

use crate::{
    store::{
        blob_store::{OwnedBlob, UnwrapSafeExt},
        Blob, BlobStore, Detached, NoError,
    },
    Hex, Lit, RadixTree,
};
use std::fmt::Debug;
#[cfg(test)]
mod tests;

const PTR_SIZE: usize = std::mem::size_of::<*const u8>();

/// An owned blob, encoded as an union of an `Arc<Vec<u8>>`, or an inline field
/// with a size up to PTR_SIZE.
///
/// Inline must be used from size 0 to PTR_SIZE inclusive, so 0..8 on 64 bit archs.
///
/// The size itself is stored externally.
union CompactOwnedBlob {
    arc: ManuallyDrop<Arc<Vec<u8>>>,
    inline: [u8; PTR_SIZE],
}

impl CompactOwnedBlob {
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
        if hdr.is_inline() {
            None
        } else {
            Some(Arc::strong_count(unsafe { &self.arc }))
        }
    }

    fn slice(&self, hdr: Header) -> &[u8] {
        unsafe {
            if hdr.is_inline() {
                &self.inline[..hdr.len()]
            } else {
                self.arc.as_ref().as_ref()
            }
        }
    }

    fn manual_clone(&self, hdr: Header) -> Self {
        unsafe {
            if hdr.is_arc() {
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
            if hdr.is_arc() {
                ManuallyDrop::drop(&mut self.arc);
            }
        }
        if cfg!(debug_assertions) {
            // just to be on the safe side, dropping twice will be like dropping a null ptr
            self.inline = [0; PTR_SIZE];
        }
    }
}

/// Reference to children for use in [OwnedTreeNode].
///
/// Children can be either
/// - inline/none
/// - an arc pointing to an id
/// - an arc pointing to owned tree nodes
///
/// Which kind of reference is valid is stored externally
/// in the child header byte.
union ChildrenRef<S> {
    arc_id: ManuallyDrop<Arc<Vec<u8>>>,
    arc_data: ManuallyDrop<Arc<Vec<TreeNode<S>>>>,
    inline: [u8; PTR_SIZE],
    p: PhantomData<S>,
}

impl<S> ChildrenRef<S> {
    const EMPTY: Self = Self {
        inline: [0u8; PTR_SIZE],
    };

    fn ref_count(&self, hdr: Header) -> Option<usize> {
        if hdr.is_arc() {
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

    fn data_from_arc(arc: Arc<Vec<TreeNode<S>>>) -> Self {
        Self {
            arc_data: ManuallyDrop::new(arc),
        }
    }

    fn deref(&self, hdr: Header) -> Result<&Arc<Vec<TreeNode<S>>>, &[u8]> {
        let len = hdr.len();
        unsafe {
            if hdr.is_id() {
                Err(if hdr.is_inline() {
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

    fn deref_mut(&mut self, hdr: Header) -> Result<&mut Arc<Vec<TreeNode<S>>>, &[u8]> {
        let len = hdr.len();
        unsafe {
            if hdr.is_id() {
                Err(if hdr.is_inline() {
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
            if hdr.is_arc() {
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
            if hdr.is_arc() {
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

/// A newtype wrapper for an u8 that provides information about
/// the type of an associated pointer. Either None, Id or Data.
///
/// The highest bit is used to distinguish between id and data, id is 1.
/// The other 7 bits are used to store the length of the following bytes.
///
/// None is encoded as an id with size 0.
///
/// Every valid byte is a valid header.
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
    // empty header, 0
    const EMPTY: Header = Header::data(0);
    // none header, 0x80 (just highest bit set)
    const NONE: Header = Header::id(0);
    // marker for data stored in an arc, 0xff
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

    /// Data is to be interpreted as id
    fn is_id(&self) -> bool {
        (self.0 & 0x80) != 0
    }

    /// Data is to be interpreted as data
    fn is_data(&self) -> bool {
        !self.is_id()
    }

    /// Data is none
    fn is_none(&self) -> bool {
        self.0 == 0x80
    }

    /// Data size is so small that it fits into a word (<=PTR_SIZE)
    fn is_inline(&self) -> bool {
        self.len() <= PTR_SIZE
    }

    /// Data size is larger than a word (>PTR_SIZE)
    fn is_arc(&self) -> bool {
        self.len() > PTR_SIZE
    }

    /// Note that this is the length that fits in the header.
    ///
    /// The actual length can be larger!
    fn len(&self) -> usize {
        self.len_u8() as usize
    }

    /// len as u8
    fn len_u8(&self) -> u8 {
        self.0 & 0x7f
    }
}

/// A self contained reference to a compact owned blob
///
/// Due to the header field we know if this refers to an id or data.
#[derive(Clone, Copy)]
struct OwnedBlobRef<'a> {
    hdr: Header,
    data: &'a CompactOwnedBlob,
}

impl<'a> Debug for OwnedBlobRef<'a> {
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

impl<'a> OwnedBlobRef<'a> {
    const EMPTY: OwnedBlobRef<'static> = OwnedBlobRef {
        hdr: Header::EMPTY,
        data: &CompactOwnedBlob::EMPTY,
    };

    fn ref_count(&self) -> Option<usize> {
        self.data.ref_count(self.hdr)
    }

    fn new(hdr: Header, data: &'a CompactOwnedBlob) -> Self {
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

    fn serialize<S: BlobStore>(
        &self,
        target: &mut Vec<u8>,
        n: usize,
        store: &S,
    ) -> Result<(), S::Error> {
        let slice = self.slice();
        if self.is_id() || slice.len() < 0x80 {
            target.push(self.hdr.into());
            target.extend_from_slice(slice);
        } else {
            let id = store.write(slice)?;
            let len = id.len() + n;
            target.push(Header::id(len).into());
            target.extend_from_slice(&slice[..n]);
            target.extend_from_slice(&id);
        }
        Ok(())
    }
}

/// A self contained reference to a borrowed id or data blob that is part of some buffer
struct BorrowedBlobRef<'a> {
    hdr: Header,
    data: &'a u8,
}

impl<'a> Debug for BorrowedBlobRef<'a> {
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

impl<'a> BorrowedBlobRef<'a> {
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

/// Reference to a value
///
/// Can refer either an owned value of an in memory node, or a borrowed value in a buffer or memory mapped file.
pub struct ValueRef<'a, S: BlobStore = Detached>(
    Result<OwnedBlobRef<'a>, BorrowedBlobRef<'a>>,
    PhantomData<S>,
);

impl<'a, S: BlobStore> Debug for ValueRef<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(value) => write!(f, "{:?}", value),
            Err(value) => write!(f, "{:?}", value),
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn downcast<S2: BlobStore>(&self) -> &ValueRef<'a, S2> {
        unsafe { std::mem::transmute(self) }
    }

    pub fn data(&self) -> Option<&[u8]> {
        if self.is_none() {
            None
        } else {
            Some(self.read().unwrap())
        }
    }
}

impl<'a, S: BlobStore> ValueRef<'a, S> {
    pub fn to_owned(&self) -> Value<S> {
        match &self.0 {
            Ok(x) => OwnedValueRef::new(*x).to_owned(),
            Err(x) => Value {
                hdr: x.hdr,
                data: CompactOwnedBlob::copy_from_slice(x.slice()),
                p: PhantomData,
            },
        }
    }

    fn detached(&self, _store: &S) -> Result<Value, S::Error>
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
        match &self.0 {
            Ok(x) => x.is_id(),
            Err(x) => x.is_id(),
        }
    }
    fn is_none(&self) -> bool {
        match &self.0 {
            Ok(x) => x.is_none(),
            Err(x) => x.is_none(),
        }
    }
    fn slice(&self) -> &'a [u8] {
        match &self.0 {
            Ok(x) => x.slice(),
            Err(x) => x.slice(),
        }
    }
}

struct OwnedValueRef<'a, S>(OwnedBlobRef<'a>, PhantomData<S>);

impl<'a> OwnedValueRef<'a, Detached> {
    pub fn downcast<S2: BlobStore>(&self) -> &OwnedValueRef<'a, S2> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a, S: BlobStore> OwnedValueRef<'a, S> {
    fn new(raw: OwnedBlobRef<'a>) -> Self {
        Self(raw, PhantomData)
    }
    fn to_owned(&self) -> Value<S> {
        Value {
            hdr: self.0.hdr,
            data: self.0.data.manual_clone(self.0.hdr),
            p: PhantomData,
        }
    }
}

impl<'a> AsRef<[u8]> for OwnedValueRef<'a, Detached> {
    fn as_ref(&self) -> &[u8] {
        self.0.data.slice(self.0.hdr)
    }
}

impl<'a> Deref for OwnedValueRef<'a, Detached> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.data.slice(self.0.hdr)
    }
}

/// An owned radix tree value
pub struct Value<S: BlobStore = Detached> {
    hdr: Header,
    data: CompactOwnedBlob,
    p: PhantomData<S>,
}

impl<S: BlobStore> Debug for Value<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.hdr.is_data() {
            write!(f, "Data{}", Hex::new(self.data.slice(self.hdr)))
        } else {
            write!(f, "Id{}", Hex::new(self.data.slice(self.hdr)))
        }
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.data.slice(self.hdr)
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data.slice(self.hdr)
    }
}

impl Value {
    pub fn downcast<S2: BlobStore>(self) -> Value<S2> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<S: BlobStore> Value<S> {
    const EMPTY: Self = Self {
        hdr: Header::NONE,
        data: CompactOwnedBlob::EMPTY,
        p: PhantomData,
    };

    fn as_value_ref(&self) -> OwnedValueRef<S> {
        OwnedValueRef::new(OwnedBlobRef {
            hdr: self.hdr,
            data: &self.data,
        })
    }

    pub fn load(&self, store: &S) -> Result<OwnedBlob, S::Error>
    where
        S: BlobStore,
    {
        match self.read() {
            Ok(data) => Ok(OwnedBlob::copy_from_slice(data)),
            Err(id) => store.read(id),
        }
    }

    pub fn set(&mut self, value: Option<&ValueRef<S>>) {
        self.data.manual_drop(self.hdr);
        match value {
            Some(ValueRef(Ok(raw), _)) => {
                self.hdr = raw.hdr;
                self.data = raw.data.manual_clone(raw.hdr);
            }
            Some(ValueRef(Err(iod), _)) => {
                self.hdr = iod.hdr;
                self.data = CompactOwnedBlob::copy_from_slice(iod.slice());
            }
            None => {
                self.hdr = Header::NONE;
                self.data = CompactOwnedBlob::EMPTY;
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

impl<S: BlobStore> Drop for Value<S> {
    fn drop(&mut self) {
        self.data.manual_drop(self.hdr);
    }
}

/// An owned tree node
#[repr(C)]
pub struct TreeNode<S> {
    /// the discriminator, 0 for owned
    discriminator: u8,
    /// header for the prefix field
    prefix_hdr: Header,
    /// header for the value field
    value_hdr: Header,
    /// header for the children field
    children_hdr: Header,
    /// prefix inline data or arc
    prefix: CompactOwnedBlob,
    /// value inline data or arc
    value: CompactOwnedBlob,
    /// children inline data or arc
    children: ChildrenRef<S>,
}

/// Define equality only for unattached trees
impl PartialEq for TreeNode<Detached> {
    fn eq(&self, other: &Self) -> bool {
        self.prefix_ref().slice() == other.prefix_ref().slice()
            && self.value_ref().slice() == other.value_ref().slice()
            && self.get_children() == other.get_children()
    }
}

impl Eq for TreeNode<Detached> {}

impl<S: BlobStore> TreeNode<S> {
    fn as_ref(&self) -> TreeNodeRef<S> {
        TreeNodeRef::owned(self)
    }
}

impl TreeNode<Detached> {
    pub fn downcast<S2: BlobStore>(&self) -> TreeNode<S2> {
        let res = self.clone();
        unsafe { std::mem::transmute(res) }
    }

    pub fn try_attached<S: BlobStore>(&self, store: &S) -> Result<TreeNode<S>, S::Error> {
        let mut data = Vec::new();
        self.serialize(&mut data, store)?;
        Ok(TreeNode::<S>::deserialize(&data).unwrap())
    }
}

impl<S: BlobStore> Debug for TreeNode<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ctext = Lit(self
            .get_children()
            .map(|c| format!("n={}", c.len()))
            .unwrap_or_else(|id| {
                if id.is_empty() {
                    "Empty".to_string()
                } else {
                    format!("Id{}", Hex::new(id))
                }
            }));
        f.debug_struct("OwnedTreeNode")
            .field("prefix", &self.prefix_ref())
            .field("value", &self.value_ref())
            .field("children", &ctext)
            .finish()
    }
}

impl<S: BlobStore> TreeNode<S> {
    fn detached(&self, store: &S) -> Result<TreeNode<Detached>, S::Error> {
        let mut res = TreeNode::EMPTY;
        res.set_prefix_slice(self.load_prefix(store)?.as_ref());
        res.set_value(self.load_value(store)?);
        if let Some(mut children) = self.load_children(store)? {
            let mut rc = Vec::new();
            while let Some(child) = children.next() {
                rc.push(child.detached(store)?);
            }
            res.set_children_arc_opt(Some(Arc::new(rc)));
        }
        Ok(res)
    }

    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error> {
        let spacer = " ".repeat(indent);
        let child_ref_count = self.children.ref_count(self.children_hdr);
        let format_ref_count =
            |x: Option<usize>| x.map(|n| format!(" rc={}", n)).unwrap_or_default();
        println!("{}TreeNode", spacer);
        println!(
            "{}  prefix={:?}{}",
            spacer,
            self.prefix_ref(),
            format_ref_count(self.prefix_ref().ref_count())
        );
        println!(
            "{}  value={:?}{}",
            spacer,
            self.value_ref(),
            format_ref_count(self.value_ref().ref_count())
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
            Ok(node.to_owned())
        } else {
            anyhow::bail!("Unable to deserialize {}!", Hex::new(data));
        }
    }

    fn serialize<S2: BlobStore>(&self, target: &mut Vec<u8>, store: &S2) -> Result<(), S2::Error> {
        self.prefix_ref().serialize(target, 1, store)?;
        self.value_ref().serialize(target, 0, store)?;
        match self.get_children() {
            Ok(children) if !children.is_empty() => {
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
            Ok(_) => {
                // todo: why can we get here at all? Empty children should always be represented as id none
                target.push(Header::NONE.into());
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
        find(store, &TreeNodeRef(Ok(self)), key, |r| {
            Ok(if let FindResult::Found(tree) = r {
                tree.value_opt().is_some()
            } else {
                false
            })
        })
    }

    /// True if key is contained in this set
    fn has_prefix(&self, prefix: &[u8], store: &S) -> Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(store, &TreeNodeRef(Ok(self)), prefix, |r| {
            Ok(match r {
                FindResult::Found(_) => true,
                FindResult::Prefix { .. } => true,
                FindResult::NotFound => false,
            })
        })
    }

    /// True if the key is a prefix of the set
    fn is_prefix(&self, prefix: &[u8], store: &S) -> Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find_is_prefix(store, &TreeNodeRef(Ok(self)), prefix, |r| {
            Ok(match r {
                FindPrefixResult::Found(tree) => tree.value_opt().is_some(),
                FindPrefixResult::IsPrefix { .. } => true,
                FindPrefixResult::NotFound => false,
            })
        })
    }
    

    fn get(&self, key: &[u8], store: &S) -> Result<Option<Value<S>>, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        find(store, &TreeNodeRef(Ok(self)), key, |r| {
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
            Err(id) => TreeNodeIter::load(id, store),
        }
    }

    fn load_children_owned(&self, store: &S) -> Result<Option<TreeNodeIter<'static, S>>, S::Error> {
        match self.get_children() {
            Ok(children) => Ok(Some(TreeNodeIter::from_arc(children.clone()))),
            Err(id) => TreeNodeIter::load(id, store),
        }
    }

    fn load_children_mut(&mut self, store: &S) -> Result<&mut Vec<TreeNode<S>>, S::Error> {
        if let Err(id) = self.get_children() {
            let mut items = Vec::new();
            if let Some(mut iter) = TreeNodeIter::load(id, store)? {
                while let Some(item) = iter.next() {
                    items.push(item.to_owned());
                }
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
        let prefix = self.load_prefix(store)?;
        let mut child = Self::EMPTY;
        child.set_prefix_slice(&prefix[n..]);
        // todo: get rid of the to_vec! truncate_prefix?
        let prefix = prefix.to_vec();
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
        debug_assert!(self.children_hdr.is_data() || self.children_hdr.is_none());
        let cc = self.child_count();
        if !self.has_value() && cc == 1 {
            let children = self.get_children_mut().expect("children must be loaded");
            let children = Arc::make_mut(children);
            let mut child = children.pop().unwrap();
            debug_assert!(!self.prefix_ref().is_id(), "prefix must be loaded");
            debug_assert!(!child.prefix_ref().is_id(), "child prefix must be loaded");
            // combine the prefix again
            let mut prefix = self.prefix_ref().slice().to_vec();
            prefix.extend_from_slice(child.prefix_ref().slice());
            self.set_prefix_slice(&prefix);
            // take value from child
            std::mem::swap(&mut self.value_hdr, &mut child.value_hdr);
            std::mem::swap(&mut self.value, &mut child.value);
            // take children from child
            std::mem::swap(&mut self.children_hdr, &mut child.children_hdr);
            std::mem::swap(&mut self.children, &mut child.children);
        }
        if !self.has_value() && self.child_count() == 0 {
            self.set_prefix_owned(OwnedBlobRef::EMPTY);
            self.children_hdr = Header::NONE;
            self.children = ChildrenRef::EMPTY;
        }
    }

    fn load_prefix(&self, store: &S) -> Result<Blob<'_>, S::Error> {
        if self.prefix_ref().is_id() {
            store.read(&self.prefix_ref().slice()[1..])
        } else {
            Ok(Blob::new(self.prefix_ref().slice()))
        }
    }

    fn load_value(&self, store: &S) -> Result<Option<Blob<'_>>, S::Error> {
        if self.value_ref().is_id() {
            if self.value_ref().is_none() {
                Ok(None)
            } else {
                store.read(self.value_ref().slice()).map(Some)
            }
        } else {
            Ok(Some(Blob::new(self.value_ref().slice())))
        }
    }
}

impl<S: BlobStore> TreeNode<S> {
    const EMPTY: Self = Self {
        discriminator: 0,
        prefix_hdr: Header::EMPTY,
        prefix: CompactOwnedBlob::EMPTY,
        value_hdr: Header::NONE,
        value: CompactOwnedBlob::EMPTY,
        children_hdr: Header::NONE,
        children: ChildrenRef::EMPTY,
    };

    fn empty_with_children(arc: Arc<Vec<TreeNode<S>>>) -> Self {
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

    fn prefix_ref(&self) -> OwnedBlobRef<'_> {
        OwnedBlobRef::new(self.prefix_hdr, &self.prefix)
    }

    fn value_ref(&self) -> OwnedBlobRef<'_> {
        OwnedBlobRef::new(self.value_hdr, &self.value)
    }

    fn first_prefix_byte(&self) -> Option<u8> {
        self.prefix_ref().slice().first().cloned()
    }

    fn is_empty(&self) -> bool {
        !self.has_value() && self.is_leaf()
    }

    fn set_prefix_owned(&mut self, prefix: OwnedBlobRef) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = prefix.hdr;
        self.prefix = prefix.data.manual_clone(prefix.hdr);
    }

    fn set_prefix_slice(&mut self, prefix: &[u8]) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = Header::data(prefix.len());
        self.prefix = CompactOwnedBlob::copy_from_slice(prefix);
    }

    fn set_value_owned(&mut self, value: OwnedBlobRef) {
        self.value.manual_drop(self.value_hdr);
        self.value_hdr = value.hdr;
        self.value = value.data.manual_clone(value.hdr);
    }

    fn set_value_borrowed(&mut self, value: BorrowedBlobRef) {
        self.value.manual_drop(self.value_hdr);
        self.value_hdr = value.hdr;
        self.value = CompactOwnedBlob::copy_from_slice(value.slice());
    }

    fn set_prefix_borrowed(&mut self, prefix: BorrowedBlobRef) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.prefix_hdr = prefix.hdr;
        self.prefix = CompactOwnedBlob::copy_from_slice(prefix.slice());
    }

    fn set_children_borrowed(&mut self, value: BorrowedBlobRef) {
        assert!(value.hdr.is_id());
        self.children.manual_drop(self.children_hdr);
        self.children_hdr = value.hdr;
        self.children = ChildrenRef::id_from_slice(value.slice());
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
            self.value = CompactOwnedBlob::copy_from_slice(value);
        } else {
            self.value_hdr = Header::NONE;
            self.value = CompactOwnedBlob::EMPTY;
        }
    }

    fn get_children(&self) -> Result<&Arc<Vec<TreeNode<S>>>, &[u8]> {
        self.children.deref(self.children_hdr)
    }

    fn get_children_mut(&mut self) -> Result<&mut Arc<Vec<TreeNode<S>>>, &[u8]> {
        self.children.deref_mut(self.children_hdr)
    }

    fn set_children_arc(&mut self, arc: Arc<Vec<TreeNode<S>>>) {
        self.children.manual_drop(self.children_hdr);
        self.children_hdr = Header::ARCDATA;
        self.children = ChildrenRef::data_from_arc(arc);
    }

    fn set_children_arc_opt(&mut self, arc_opt: Option<Arc<Vec<TreeNode<S>>>>) {
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
            Some(OwnedValueRef::new(self.value_ref()))
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

    fn take_value(&mut self) -> Value<S> {
        let mut value = Value::EMPTY;
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
                    // todo: what to do here? Why does this even happen?
                    2
                }
            }
        }
    }
}

impl<S> Drop for TreeNode<S> {
    fn drop(&mut self) {
        self.prefix.manual_drop(self.prefix_hdr);
        self.value.manual_drop(self.value_hdr);
        self.children.manual_drop(self.children_hdr);
    }
}

impl<S> Clone for TreeNode<S> {
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

/// A borrowed tree node
///
/// This is a struct that contains pointers to the raw data of a node,
/// typically in a memory mapped file or a buffer.
///
/// Total size should be 4*PTR_SIZE, so 32 bytes on 64 bit
#[repr(C)]
struct BorrowedTreeNode<'a, S: BlobStore> {
    discriminator: u8,
    prefix_hdr: Header,
    value_hdr: Header,
    children_hdr: Header,
    /// reference to the first prefix byte
    prefix: &'a u8,
    /// reference to the first value byte
    value: &'a u8,
    /// reference to the first children byte
    children: &'a u8,
    p: PhantomData<&'a S>,
}

impl<'a, S: BlobStore> Clone for BorrowedTreeNode<'a, S> {
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

impl<'a, S: BlobStore> Copy for BorrowedTreeNode<'a, S> {}

impl<'a, S: BlobStore> Debug for BorrowedTreeNode<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedTreeNode")
            .field("prefix", &self.prefix_ref())
            .field("value", &self.value_ref())
            .field("children_hdr", &self.children_ref())
            .finish()
    }
}

const EMPTY_BYTES: &[u8] = &[Header::EMPTY.0, Header::NONE.0, Header::NONE.0];

impl<S: BlobStore + 'static> BorrowedTreeNode<'static, S> {
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

impl<'a, S: BlobStore> BorrowedTreeNode<'a, S> {
    #[allow(clippy::wrong_self_convention)]
    fn to_owned(&self) -> TreeNode<S> {
        let mut res = TreeNode::EMPTY;
        res.set_prefix_borrowed(self.prefix_ref());
        res.set_value_borrowed(self.value_ref());
        res.set_children_id(self.children_ref().read().unwrap_err());
        res
    }

    fn detached(&self, store: &S) -> Result<TreeNode<Detached>, S::Error>
    where
        S: BlobStore,
    {
        self.to_owned().detached(store)
    }

    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error>
    where
        S: BlobStore,
    {
        let spacer = " ".repeat(indent);
        println!("{}TreeNode", spacer);
        println!("{}  prefix={:?}", spacer, self.prefix_ref(),);
        println!("{}  value={:?}", spacer, self.value_ref(),);
        let iter = self.load_children(store)?;
        println!("{}", spacer);
        if let Some(mut iter) = iter {
            while let Some(child) = iter.next() {
                child.dump(indent + 4, store)?;
            }
        }
        Ok(())
    }

    fn first_prefix_byte(&self) -> Option<u8> {
        self.prefix_ref().slice().first().cloned()
    }

    fn prefix_ref(&self) -> BorrowedBlobRef<'a> {
        BorrowedBlobRef::new(self.prefix_hdr, self.prefix)
    }

    fn value_ref(&self) -> BorrowedBlobRef<'a> {
        BorrowedBlobRef::new(self.value_hdr, self.value)
    }

    fn children_ref(&self) -> BorrowedBlobRef<'a> {
        BorrowedBlobRef::new(self.children_hdr, self.children)
    }

    fn bytes_len(&self) -> usize {
        self.prefix_hdr.len() + self.value_hdr.len() + self.children_hdr.len() + 3
    }

    fn load_prefix(&self, store: &S) -> Result<Blob, S::Error>
    where
        S: BlobStore,
    {
        match self.prefix_ref().read() {
            Ok(data) => Ok(Blob::new(data)),
            Err(id) => store.read(&id[1..]),
        }
    }

    fn value_ref_opt(&self) -> Option<ValueRef<S>> {
        if self.value_hdr != Header::NONE {
            Some(ValueRef(Err(self.value_ref()), PhantomData))
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
            let id = self.children_ref().slice();
            TreeNodeIter::load(id, store)
        }
    }

    /// Read one borrowd tree node from a buffer
    pub fn read(buffer: &'a [u8]) -> Option<Self> {
        Some(Self::read_one(buffer)?.0)
    }

    /// Read one borrowd tree node from a buffer, and return the remainder of the buffer
    pub fn read_one(rest: &'a [u8]) -> Option<(Self, &'a [u8])> {
        let prefix_hdr = Header::from(*rest.first()?);
        let len = prefix_hdr.len() + 1;
        if rest.len() < len {
            return None;
        }
        let (prefix, rest) = (&rest[0], &rest[len..]);

        let value_hdr = Header::from(*rest.first()?);
        let len = value_hdr.len() + 1;
        if rest.len() < len {
            return None;
        }
        let (value, rest) = (&rest[0], &rest[len..]);

        let children_hdr = Header::from(*rest.first()?);
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
            rest,
        ))
    }
}

/// A tree node ref, either a reference to an OwnedTreeNode, or a BorrowedTreeNode
#[derive(Clone, Copy)]
pub struct TreeNodeRef<'a, S: BlobStore = Detached>(
    Result<&'a TreeNode<S>, BorrowedTreeNode<'a, S>>,
);

impl<'a, S: BlobStore> Debug for TreeNodeRef<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.dispatch() {
            Ok(owned) => f.debug_tuple("Owned").field(owned).finish(),
            Err(borrowed) => f.debug_tuple("Borrowed").field(&borrowed).finish(),
        }
    }
}

impl<'a> TreeNodeRef<'a, Detached> {
    pub fn downcast<S2: BlobStore>(&self) -> TreeNode<S2> {
        match self.dispatch() {
            Ok(owned) => owned.downcast(),
            Err(_) => unreachable!(),
        }
    }
}

impl<'a, S: BlobStore> TreeNodeRef<'a, S> {
    fn owned(x: &'a TreeNode<S>) -> Self {
        Self(Ok(x))
    }

    fn borrowed(x: BorrowedTreeNode<'a, S>) -> Self {
        Self(Err(x))
    }

    fn detached(&self, store: &S) -> Result<TreeNode<Detached>, S::Error> {
        match self.dispatch() {
            Ok(inner) => inner.detached(store),
            Err(inner) => inner.detached(store),
        }
    }

    fn dispatch(&self) -> Result<&'a TreeNode<S>, BorrowedTreeNode<S>> {
        match self.0 {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(inner),
        }
    }

    fn to_owned(&self) -> TreeNode<S> {
        match self.dispatch() {
            Ok(owned) => owned.clone(),
            Err(borrowed) => borrowed.to_owned(),
        }
    }

    fn dump(self, indent: usize, store: &S) -> Result<(), S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.dump(indent, store),
            Err(borrowed) => borrowed.dump(indent, store),
        }
    }

    fn load_prefix(&self, store: &S) -> Result<Blob, S::Error> {
        match &self.0 {
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
        match self.dispatch() {
            Ok(owned) => owned.load_children_owned(store),
            Err(borrowed) => borrowed.load_children(store),
        }
    }

    fn value_opt(&self) -> Option<ValueRef<S>> {
        match &self.0 {
            Ok(owned) => owned.value_opt().map(|x| ValueRef(Ok(x.0), PhantomData)),
            Err(borrowed) => borrowed.value_ref_opt(),
        }
    }

    fn clone_shortened(&self, store: &S, n: usize) -> Result<TreeNode<S>, S::Error> {
        match self.dispatch() {
            Ok(owned) => owned.clone_shortened(store, n),
            Err(_borrowed) => todo!(),
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

enum FindPrefixResult<T> {
    // Found an exact match
    Found(T),
    // did not find anything, T is the closest match, with n remaining (unmatched) in the prefix of T
    NotFound,
    IsPrefix
}

/// find a prefix in a tree. Will either return
/// - Found(tree) if we found the tree exactly,
/// - Prefix if we found a tree of which prefix is a prefix
/// - NotFound if there is no tree
fn find_is_prefix<S: BlobStore, T>(
    store: &S,
    tree: &TreeNodeRef<S>,
    prefix: &[u8],
    f: impl FnOnce(FindPrefixResult<&TreeNodeRef<S>>) -> Result<T, S::Error>,
) -> Result<T, S::Error> {
    let tree_prefix = tree.load_prefix(store)?;
    let n = common_prefix(&tree_prefix, prefix);
    // remaining in tree prefix
    let rt = tree_prefix.len() - n;
    // remaining in prefix
    let rp = prefix.len() - n;
    let fr = if rp == 0 && rt == 0 {
        // direct hit, however need to check if this node has a value
        FindPrefixResult::Found(tree)
    } else if rp == 0 {
        // tree does not contain a key that is a direct prefix of the string
        FindPrefixResult::NotFound
    } else if rt == 0 {
        // Here we can check if the current node is has a key,
        // If yes, FindResult is IsPrefix
        if tree.value_opt().is_some() {
            FindPrefixResult::IsPrefix
        } else {
            // else we keep looking
            let c = prefix[n];
            let tree_children = tree.load_children(store)?;
            if let Some(tree_children) = tree_children {
                if let Some(child) = tree_children.find(c) {
                    return find_is_prefix(store, &child, &prefix[n..], f);
                } else {
                    FindPrefixResult::NotFound
                }
            } else {
                FindPrefixResult::NotFound
            }
        }
    } else {
        // disjoint, but we still need to store how far we matched
        FindPrefixResult::NotFound
    };
    f(fr)
}

/// Return the subtree with the given prefix. Will return an empty tree in case there is no match.
fn filter_prefix<S: BlobStore>(
    node: &TreeNodeRef<S>,
    store: &S,
    prefix: &[u8],
    substitution: &[u8],
) -> Result<TreeNode<S>, S::Error> {
    find(store, node, prefix, |x: FindResult<&TreeNodeRef<'_, S>>| {
        Ok(match x {
            FindResult::Found(res) => {
                let mut res = res.to_owned();
                res.set_prefix_slice(substitution);
                res
            }
            FindResult::Prefix { tree, matching } => {
                let tree_prefix = tree.load_prefix(store)?;
                let mut res = tree.to_owned();
                let mut tmp = Vec::new();
                tmp.extend_from_slice(substitution);
                tmp.extend_from_slice(&tree_prefix[matching..]);
                res.set_prefix_slice(&tmp);
                res
            }
            FindResult::NotFound => TreeNode::EMPTY,
        })
    })
}

/// get the first value
fn first_value<S: BlobStore>(
    node: &TreeNodeRef<S>,
    store: &S,
) -> Result<Option<Value<S>>, S::Error> {
    Ok(match node.value_opt() {
        Some(value) => Some(value.to_owned()),
        None => match node.load_children(store)? {
            Some(mut children) => first_value(&children.next().unwrap(), store)?,
            None => None,
        },
    })
}

/// get the last value
fn last_value<S: BlobStore>(
    node: &TreeNodeRef<S>,
    store: &S,
) -> Result<Option<Value<S>>, S::Error> {
    Ok(match node.load_children(store)? {
        Some(mut children) => last_value(&children.last().unwrap(), store)?,
        None => node.value_opt().map(|x| x.to_owned()),
    })
}

/// get the first value
fn first_entry<S: BlobStore>(
    mut prefix: Vec<u8>,
    node: &TreeNodeRef<S>,
    store: &S,
) -> Result<Option<(Vec<u8>, Value<S>)>, S::Error> {
    prefix.extend_from_slice(&node.load_prefix(store)?);
    Ok(match node.value_opt() {
        Some(value) => Some((prefix, value.to_owned())),
        None => match node.load_children(store)? {
            Some(mut children) => first_entry(prefix, &children.next().unwrap(), store)?,
            None => None,
        },
    })
}

/// get the last value
fn last_entry<S: BlobStore>(
    mut prefix: Vec<u8>,
    node: &TreeNodeRef<S>,
    store: &S,
) -> Result<Option<(Vec<u8>, Value<S>)>, S::Error> {
    prefix.extend_from_slice(&node.load_prefix(store)?);
    Ok(match node.load_children(store)? {
        Some(mut children) => last_entry(prefix, &children.last().unwrap(), store)?,
        None => node.value_opt().map(|x| (prefix, x.to_owned())),
    })
}

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

#[cfg_attr(feature = "custom-store", visibility::make(pub))]
/// Converter that converts nodes from one kind of store to another
trait NodeConverter<A, B: BlobStore> {
    fn convert_node(&self, node: &TreeNodeRef<A>, store: &A) -> Result<TreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<B>, A::Error>
    where
        A: BlobStore;
    fn convert_value(&self, bv: &ValueRef<A>, store: &A) -> Result<Value<B>, A::Error>
    where
        A: BlobStore;
}

/// Converter that converts from a node with NoStore to any other store.
///
/// This is always possible and a noop.
#[cfg_attr(feature = "custom-store", visibility::make(pub))]
#[derive(Clone, Copy)]
struct DowncastConverter;

impl<B: BlobStore> NodeConverter<Detached, B> for DowncastConverter {
    fn convert_node(
        &self,
        node: &TreeNodeRef<Detached>,
        _: &Detached,
    ) -> Result<TreeNode<B>, NoError> {
        Ok(node.downcast())
    }

    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<Detached>,
        store: &Detached,
        n: usize,
    ) -> Result<TreeNode<B>, NoError> {
        node.clone_shortened(store, n).map(|x| x.downcast())
    }

    fn convert_value(&self, bv: &ValueRef, _: &Detached) -> Result<Value<B>, NoError> {
        Ok(bv.downcast::<B>().to_owned())
    }
}

/// Identity node converter to simplify plumbing
#[derive(Clone, Copy)]
#[cfg_attr(feature = "custom-store", visibility::make(pub))]
struct IdentityConverter;

impl<A: BlobStore> NodeConverter<A, A> for IdentityConverter {
    fn convert_node(&self, node: &TreeNodeRef<A>, _store: &A) -> Result<TreeNode<A>, A::Error> {
        Ok(node.to_owned())
    }

    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<A>, A::Error> {
        node.clone_shortened(store, n)
    }

    fn convert_value(&self, bv: &ValueRef<A>, _store: &A) -> Result<Value<A>, A::Error> {
        Ok(bv.to_owned())
    }
}

/// A converter that converts from one store to another store by just completely detaching it
#[derive(Clone, Copy)]
#[cfg_attr(feature = "custom-store", visibility::make(pub))]
struct DetachConverter;

impl<A: BlobStore, B: BlobStore> NodeConverter<A, B> for DetachConverter {
    fn convert_node(&self, node: &TreeNodeRef<A>, store: &A) -> Result<TreeNode<B>, A::Error> {
        Ok(node.detached(store)?.downcast())
    }
    fn convert_value(&self, value: &ValueRef<A>, store: &A) -> Result<Value<B>, A::Error> {
        Ok(value.detached(store)?.downcast())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNodeRef<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<B>, A::Error> {
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
            #[allow(clippy::manual_map)]
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

fn cmp<A: BlobStore, B: BlobStore>(
    a: &InPlaceVecBuilder<'_, TreeNode<A>>,
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

struct OwnedTreeNodeIter<'a, S>(Option<Arc<Vec<TreeNode<S>>>>, slice::Iter<'a, TreeNode<S>>);

impl<S: BlobStore> OwnedTreeNodeIter<'static, S> {
    fn new_owned(owner: Arc<Vec<TreeNode<S>>>) -> Self {
        let iter = unsafe { extend_lifetime(owner.as_ref()) }.iter();
        Self(Some(owner.clone()), iter)
    }
}

fn is_no_store<S: 'static>() -> bool {
    TypeId::of::<S>() == TypeId::of::<Detached>()
}

impl<'a, S: BlobStore> OwnedTreeNodeIter<'a, S> {
    fn new(slice: &'a [TreeNode<S>]) -> Self {
        Self(None, slice.iter())
    }

    fn into_owned(mut self) -> Option<Arc<Vec<TreeNode<S>>>> {
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

    fn detached(mut self, store: &S) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, S::Error> {
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

    fn find(&self, prefix: u8) -> Option<&TreeNode<S>> {
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

    fn next(&mut self) -> Option<&TreeNode<S>> {
        self.1.next()
    }

    fn last(&mut self) -> Option<&TreeNode<S>> {
        self.1.as_slice().last()
    }
}

struct BorrowedTreeNodeIter<S> {
    data: OwnedBlob,
    offset: usize,
    record_size: u8,
    p: PhantomData<S>,
}

impl<S: BlobStore> BorrowedTreeNodeIter<S> {
    fn load(id: &[u8], store: &S) -> Result<Option<Self>, S::Error> {
        Ok(if id.is_empty() {
            None
        } else {
            let (record_size, data) = (id[0], store.read(&id[1..])?);
            Some(Self {
                data,
                offset: 0,
                record_size,
                p: PhantomData,
            })
        })
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_owned(self) -> Option<Arc<Vec<TreeNode<S>>>> {
        if self.is_empty() {
            None
        } else {
            todo!()
        }
    }

    fn detached(self, _store: &S) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, S::Error> {
        Ok(if self.is_empty() { None } else { todo!() })
    }

    fn is_empty(&self) -> bool {
        self.offset == self.data.len()
    }

    fn find(&self, prefix: u8) -> Option<BorrowedTreeNode<S>> {
        let _elems = if self.record_size != 0 {
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

    fn last(&mut self) -> Option<BorrowedTreeNode<S>> {
        let mut offset = self.offset;
        let mut last = None;
        // todo: special case for when we have a record size!
        while offset < self.data.len() {
            if let Some(x) = BorrowedTreeNode::<S>::read(&self.data[offset..]) {
                last = Some(x);
                offset += x.bytes_len();
            } else {
                break;
            }
        }
        last
    }
}

enum TreeNodeIter<'a, S> {
    Owned(OwnedTreeNodeIter<'a, S>),
    Borrowed(BorrowedTreeNodeIter<S>),
}

impl<S: BlobStore> TreeNodeIter<'static, S> {
    fn from_arc(arc: Arc<Vec<TreeNode<S>>>) -> Self {
        Self::Owned(OwnedTreeNodeIter::new_owned(arc))
    }
}

impl<'a, S: BlobStore> TreeNodeIter<'a, S> {
    fn load(id: &[u8], store: &S) -> Result<Option<Self>, S::Error> {
        Ok(BorrowedTreeNodeIter::load(id, store)?.map(Self::Borrowed))
    }

    fn from_slice(slice: &'a [TreeNode<S>]) -> Option<Self> {
        if !slice.is_empty() {
            Some(Self::Owned(OwnedTreeNodeIter::new(slice)))
        } else {
            None
        }
    }

    fn into_owned(self) -> Option<Arc<Vec<TreeNode<S>>>> {
        match self {
            Self::Owned(x) => x.into_owned(),
            Self::Borrowed(x) => x.to_owned(),
        }
    }

    fn detached(self, store: &S) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, S::Error> {
        match self {
            Self::Owned(x) => x.detached(store),
            Self::Borrowed(x) => x.detached(store),
        }
    }

    fn find(&self, prefix: u8) -> Option<TreeNodeRef<S>> {
        match self {
            Self::Owned(x) => x.find(prefix).map(|x| TreeNodeRef::owned(x)),
            Self::Borrowed(x) => x.find(prefix).map(|x| TreeNodeRef::borrowed(x)),
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
            Self::Owned(x) => x.next().map(|x| TreeNodeRef::owned(x)),
            Self::Borrowed(x) => x.next().map(|x| TreeNodeRef::borrowed(x)),
        }
    }

    fn last(&mut self) -> Option<TreeNodeRef<'_, S>> {
        match self {
            Self::Owned(x) => x.last().map(|x| TreeNodeRef::owned(x)),
            Self::Borrowed(x) => x.last().map(|x| TreeNodeRef::borrowed(x)),
        }
    }
}

fn scan_prefix<S: BlobStore + Clone>(
    store: S,
    tree: &TreeNodeRef<S>,
    prefix: &[u8],
) -> Result<KeyValueIter<S>, S::Error> {
    let store1 = store.clone();
    find(&store, tree, prefix, |r| {
        Ok(match r {
            FindResult::Found(tree) => {
                let prefix = IterKey::new(prefix);
                let tree: TreeNode<S> = tree.to_owned();
                KeyValueIter::new(TreeNodeIter::from_arc(Arc::new(vec![tree])), store1, prefix)
            }
            FindResult::Prefix { tree, matching } => {
                let prefix = IterKey::new(&prefix[..prefix.len() - matching]);
                let tree: TreeNode<S> = tree.to_owned();
                KeyValueIter::new(TreeNodeIter::from_arc(Arc::new(vec![tree])), store1, prefix)
            }
            FindResult::NotFound => KeyValueIter::empty(store1),
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
) -> Result<TreeNode<Detached>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let value;
    let children;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if let Some(bv) = b.value_opt() {
            if let Some(av) = a.value_opt() {
                f(&av, &bv)?
            } else {
                Some(bv.detached(&bb)?)
            }
        } else {
            a.value_opt().map(|x| x.detached(&ab)).transpose()?
        };
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        children = outer_combine_children(ac, ab, bc, bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = a.value_opt().map(|x| x.detached(&ab)).transpose()?;
        let ac = a.load_children(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        children = outer_combine_children(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = b.value_opt().map(|x| x.detached(&bb)).transpose()?;
        let ac = [a.clone_shortened(&ab, n)?];
        let bc = b.load_children(&bb)?;
        children = outer_combine_children(TreeNodeIter::from_slice(&ac), ab, bc, bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = None;
        // children is just the shortened children a and b in the right order
        let a = DetachConverter.convert_node_shortened(a, &ab, n)?;
        let b = DetachConverter.convert_node_shortened(b, &bb, n)?;
        let vec = if ap[n] > bp[n] {
            vec![b, a]
        } else {
            vec![a, b]
        };
        children = Some(Arc::new(vec));
    }
    let mut res = TreeNode::EMPTY;
    res.set_prefix_slice(&ap[..n]);
    res.set_value(value);
    res.set_children_arc_opt(children);
    res.canonicalize();
    Ok(res)
}

fn outer_combine_children<'a, A, B, E, F>(
    ac: Option<TreeNodeIter<'a, A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
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
                if !r.is_empty() {
                    res.push(r);
                }
            }
            if res.is_empty() {
                None
            } else {
                Some(Arc::new(res))
            }
        }
        (None, Some(bc)) => bc.detached(&bb)?,
        (Some(ac), None) => ac.detached(&ab)?,
        (None, None) => None,
    })
}

/// Outer combine two trees with a function f
fn outer_combine_with<A, B, C, F>(
    a: &mut TreeNode<A>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == bp.len() {
        // ensure that prefixes are identical even if ap.len() > n
        if n != ap.len() {
            a.split(&ab, n)?;
        }
        // prefixes are now identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_owned(av.as_value_ref().0);
            } else {
                let av = c.convert_value(&bv, &bb)?;
                a.set_value_owned(av.as_value_ref().0);
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
    ac: &'a mut Vec<TreeNode<A>>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
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

/// Outer combine two trees with a function f
fn inner_combine<A, B, E, F>(
    a: &TreeNodeRef<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<TreeNode<Detached>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let value;
    let children;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if let Some(bv) = b.value_opt() {
            if let Some(av) = a.value_opt() {
                f(&av, &bv)?
            } else {
                None
            }
        } else {
            None
        };
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        children = inner_combine_children(ac, ab, bc, bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = None;
        let ac = a.load_children(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        children = inner_combine_children(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = None;
        let ac = [a.clone_shortened(&ab, n)?];
        let bc = b.load_children(&bb)?;
        children = inner_combine_children(TreeNodeIter::from_slice(&ac), ab, bc, bb, f)?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = None;
        // children is none
        children = None;
    }
    let mut res = TreeNode::EMPTY;
    res.set_prefix_slice(&ap[..n]);
    res.set_value(value);
    res.set_children_arc_opt(children);
    res.canonicalize();
    Ok(res)
}

fn inner_combine_children<'a, A, B, E, F>(
    ac: Option<TreeNodeIter<'a, A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
{
    Ok(match (ac, bc) {
        (Some(ac), Some(bc)) => {
            let mut res = Vec::new();
            let mut iter = OuterJoin::<A, B, E>::new(ac, bc);
            while let Some(x) = iter.next() {
                if let (Some(a), Some(b)) = x? {
                    let r = inner_combine(&a, ab.clone(), &b, bb.clone(), f)?;
                    if !r.is_empty() {
                        res.push(r);
                    }
                }
            }
            if res.is_empty() {
                None
            } else {
                Some(Arc::new(res))
            }
        }
        _ => None,
    })
}

/// Inner combine two trees with a predicate f
fn inner_combine_pred<A, B, E, F>(
    a: &TreeNodeRef<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<bool, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        if let (Some(av), Some(bv)) = (a.value_opt(), b.value_opt()) {
            if f(&av, &bv)? {
                return Ok(true);
            }
        }
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        return inner_combine_children_pred(ac, ab, bc, bb, f);
    } else if n == ap.len() {
        let bc = b.clone_shortened(&bb, n)?;
        let bc = TreeNodeRef::owned(&bc);
        if let Some(mut ac) = a.load_children(&ab)? {
            while let Some(ac) = ac.next() {
                if inner_combine_pred(&ac, ab.clone(), &bc, bb.clone(), f)? {
                    return Ok(true);
                }
            }
        }
    } else if n == bp.len() {
        let ac = a.clone_shortened(&ab, n)?;
        let ac = TreeNodeRef::owned(&ac);
        if let Some(mut bc) = b.load_children(&bb)? {
            while let Some(bc) = bc.next() {
                if inner_combine_pred(&ac, ab.clone(), &bc, bb.clone(), f)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn inner_combine_children_pred<A, B, E, F>(
    ac: Option<TreeNodeIter<A>>,
    ab: A,
    bc: Option<TreeNodeIter<B>>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<bool, E> + Copy,
{
    if let (Some(ac), Some(bc)) = (ac, bc) {
        let mut iter = OuterJoin::<A, B, E>::new(ac, bc);
        while let Some(x) = iter.next() {
            if let (Some(a), Some(b)) = x? {
                if inner_combine_pred(&a, ab.clone(), &b, bb.clone(), f)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// Outer combine two trees with a function f
fn inner_combine_with<A, B, C, F>(
    a: &mut TreeNode<A>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are now identical
        if let (Some(mut av), Some(bv)) = (a.take_value_opt(), b.value_opt()) {
            f(&mut av, &bv)?;
            a.set_value_owned(av.as_value_ref().0);
        }
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        inner_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is none
        a.set_value_slice(None);
        let ac = a.load_children_mut(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        inner_combine_children_with(ac, ab, TreeNodeIter::from_slice(&bc), bb, c, f)?;
    } else if n == bp.len() {
        // b is a prefix of b
        a.split(&ab, n)?;
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        inner_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else {
        // the two nodes are disjoint
        a.set_value_slice(None);
        a.set_children_arc_opt(None);
    }
    a.canonicalize();
    Ok(())
}

fn inner_combine_children_with<'a, A, B, C, F>(
    ac: &'a mut Vec<TreeNode<A>>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
    A::Error: From<B::Error>,
{
    if let Some(bc) = bc {
        if ac.is_empty() {
            return Ok(());
        }
        let mut acb = InPlaceVecBuilder::from(ac);
        let mut bci = bc;
        while let Some(ordering) = cmp(&acb, &mut bci) {
            match ordering {
                Ordering::Less => {
                    acb.consume(1, false);
                }
                Ordering::Equal => {
                    // the .unwrap() are safe because cmp guarantees that there is a value on both sides
                    let ac = acb.source_slice_mut().get_mut(0).unwrap();
                    let bc = bci.next().unwrap();
                    inner_combine_with(ac, ab.clone(), &bc, bb.clone(), c.clone(), f)?;
                    // only move if the child is non-empty
                    let non_empty = !ac.is_empty();
                    acb.consume(1, non_empty);
                }
                Ordering::Greater => {
                    // the .unwrap() is safe because cmp guarantees that there is a value
                    let _ = bci.next().unwrap();
                }
            }
        }
    } else {
        ac.clear();
    }
    Ok(())
}

/// Outer combine two trees with a function f
fn left_combine<A, B, E, F>(
    a: &TreeNodeRef<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<TreeNode<Detached>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let value;
    let children;
    let mut res = TreeNode::EMPTY;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if let Some(av) = a.value_opt() {
            if let Some(bv) = b.value_opt() {
                f(&av, &bv)?
            } else {
                Some(av.detached(&ab)?)
            }
        } else {
            None
        };
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        children = left_combine_children(ac, ab, bc, bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = a.value_opt().map(|x| x.detached(&ab)).transpose()?;
        let ac = a.load_children(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        children = left_combine_children(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = None;
        let ac = [a.clone_shortened(&ab, n)?];
        let bc = b.load_children(&bb)?;
        children = left_combine_children(TreeNodeIter::from_slice(&ac), ab, bc, bb, f)?;
    } else {
        return Ok(a.detached(&ab)?);
    }
    res.set_prefix_slice(&ap[..n]);
    res.set_value(value);
    res.set_children_arc_opt(children);
    res.canonicalize();
    Ok(res)
}

fn left_combine_children<'a, A, B, E, F>(
    ac: Option<TreeNodeIter<'a, A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<Option<Arc<Vec<TreeNode<Detached>>>>, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<Option<Value>, E> + Copy,
{
    Ok(match (ac, bc) {
        (Some(ac), Some(bc)) => {
            let mut res = Vec::new();
            let mut iter = OuterJoin::<A, B, E>::new(ac, bc);
            while let Some(x) = iter.next() {
                match x? {
                    (Some(a), Some(b)) => {
                        let r = left_combine(&a, ab.clone(), &b, bb.clone(), f)?;
                        if !r.is_empty() {
                            res.push(r);
                        }
                    }
                    (Some(a), None) => {
                        res.push(a.detached(&ab)?);
                    }
                    _ => {}
                };
            }
            if res.is_empty() {
                None
            } else {
                Some(Arc::new(res))
            }
        }
        (Some(ac), None) => ac.detached(&ab)?,
        _ => None,
    })
}

/// Inner combine two trees with a predicate f
fn left_combine_pred<A, B, E, F>(
    a: &TreeNodeRef<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<bool, E> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        match (a.value_opt(), b.value_opt()) {
            (Some(av), Some(bv)) => {
                if f(&av, &bv)? {
                    return Ok(true);
                }
            }
            (Some(_), None) => return Ok(true),
            _ => {}
        }
        let ac = a.load_children(&ab)?;
        let bc = b.load_children(&bb)?;
        left_combine_children_pred(ac, ab, bc, bb, f)
    } else if n == ap.len() {
        if a.value_opt().is_some() {
            return Ok(true);
        };
        let ac = a.load_children(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        left_combine_children_pred(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)
    } else if n == bp.len() {
        let ac = [a.clone_shortened(&ab, n)?];
        let bc = b.load_children(&bb)?;
        left_combine_children_pred(TreeNodeIter::from_slice(&ac), ab, bc, bb, f)
    } else {
        Ok(true)
    }
}

fn left_combine_children_pred<A, B, E, F>(
    ac: Option<TreeNodeIter<A>>,
    ab: A,
    bc: Option<TreeNodeIter<B>>,
    bb: B,
    f: F,
) -> Result<bool, E>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    E: From<A::Error> + From<B::Error>,
    F: Fn(&ValueRef<A>, &ValueRef<B>) -> Result<bool, E> + Copy,
{
    match (ac, bc) {
        (Some(ac), Some(bc)) => {
            let mut iter = OuterJoin::<A, B, E>::new(ac, bc);
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
        }
        (Some(_), None) => return Ok(true),
        _ => {}
    }
    Ok(false)
}

/// Outer combine two trees with a function f
fn left_combine_with<A, B, C, F>(
    a: &mut TreeNode<A>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are now identical
        if let Some(bv) = b.value_opt() {
            if let Some(mut av) = a.take_value_opt() {
                f(&mut av, &bv)?;
                a.set_value_owned(av.as_value_ref().0);
            }
        }
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        left_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        let ac = a.load_children_mut(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        left_combine_children_with(ac, ab, TreeNodeIter::from_slice(&bc), bb, c, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        a.split(&ab, n)?;
        let ac = a.load_children_mut(&ab)?;
        let bc = b.load_children(&bb)?;
        left_combine_children_with(ac, ab, bc, bb, c, f)?;
    } else {
        // the two nodes are disjoint
    }
    a.canonicalize();
    Ok(())
}

fn left_combine_children_with<'a, A, B, C, F>(
    ac: &'a mut Vec<TreeNode<A>>,
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
    F: Fn(&mut Value<A>, &ValueRef<B>) -> Result<(), A::Error> + Copy,
    A::Error: From<B::Error>,
{
    if let Some(bc) = bc {
        if ac.is_empty() {
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
                    left_combine_with(ac, ab.clone(), &bc, bb.clone(), c.clone(), f)?;
                    // only move if the child is non-empty
                    let non_empty = !ac.is_empty();
                    acb.consume(1, non_empty);
                }
                Ordering::Greater => {
                    // the .unwrap() is safe because cmp guarantees that there is a value
                    let _ = bci.next().unwrap();
                }
            }
        }
    }
    Ok(())
}

/// Retain all parts of the tree for which that contains a prefix.
///
/// The predicate `f` is used to filter the tree `that` before applying it.
/// If the predicate returns always false, this will result in the empty tree.
fn retain_prefix_with<A, B, F>(
    a: &mut TreeNode<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    F: Fn(&ValueRef<B>) -> Result<bool, A::Error> + Copy,
    A::Error: From<B::Error>,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        if b.value_opt().is_none() || !f(&b.value_opt().unwrap())? {
            a.set_value_slice(None);
            let ac = a.load_children_mut(&ab)?;
            let bc = b.load_children(&bb)?;
            retain_prefix_children_with(ac, ab, bc, bb, f)?;
        }
    } else if n == bp.len() {
        // that is a prefix of self
        if b.value_opt().is_none() {
            a.split(&ab, n)?;
            let ac = a.load_children_mut(&ab)?;
            let bc = b.load_children(&bb)?;
            retain_prefix_children_with(ac, ab, bc, bb, f)?;
        } else if !f(&b.value_opt().unwrap())? {
            a.set_value_slice(None);
            a.set_children_arc_opt(None);
        }
    } else if n == ap.len() {
        // self is a prefix of that
        a.set_value_slice(None);
        let ac = a.load_children_mut(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        retain_prefix_children_with(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)?;
    } else {
        // disjoint, nuke it
        a.set_value_slice(None);
        a.set_children_arc_opt(None);
    }
    a.canonicalize();
    Ok(())
}

fn retain_prefix_children_with<'a, A, B, F>(
    ac: &'a mut Vec<TreeNode<A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    F: Fn(&ValueRef<B>) -> Result<bool, A::Error> + Copy,
    A::Error: From<B::Error>,
{
    if let Some(bc) = bc {
        if ac.is_empty() {
            return Ok(());
        }
        let mut acb = InPlaceVecBuilder::from(ac);
        let mut bci = bc;
        while let Some(ordering) = cmp(&acb, &mut bci) {
            match ordering {
                Ordering::Less => {
                    acb.consume(1, false);
                }
                Ordering::Equal => {
                    // the .unwrap() are safe because cmp guarantees that there is a value on both sides
                    let ac = acb.source_slice_mut().get_mut(0).unwrap();
                    let bc = bci.next().unwrap();
                    retain_prefix_with(ac, ab.clone(), &bc, bb.clone(), f)?;
                    // only move if the child is non-empty
                    let non_empty = !ac.is_empty();
                    acb.consume(1, non_empty);
                }
                Ordering::Greater => {
                    // the .unwrap() is safe because cmp guarantees that there is a value
                    let _ = bci.next().unwrap();
                }
            }
        }
    } else {
        ac.clear();
    }
    Ok(())
}

/// Retain all parts of the tree for which that contains a prefix.
///
/// The predicate `f` is used to filter the tree `that` before applying it.
/// If the predicate returns always false, this will result in the empty tree.
fn remove_prefix_with<A, B, F>(
    a: &mut TreeNode<A>,
    ab: A,
    b: &TreeNodeRef<B>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    F: Fn(&ValueRef<B>) -> Result<bool, A::Error> + Copy,
    A::Error: From<B::Error>,
{
    let ap = a.load_prefix(&ab)?;
    let bp = b.load_prefix(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        if b.value_opt().is_some() && f(&b.value_opt().unwrap())? {
            // nuke it
            a.set_value_slice(None);
            a.set_children_arc_opt(None);
        } else {
            // recurse
            let ac = a.load_children_mut(&ab)?;
            let bc = b.load_children(&bb)?;
            remove_prefix_children_with(ac, ab, bc, bb, f)?;
        }
    } else if n == bp.len() {
        // that is a prefix of self
        if b.value_opt().is_some() && f(&b.value_opt().unwrap())? {
            // nuke it
            a.set_value_slice(None);
            a.set_children_arc_opt(None);
        } else {
            // recurse
            a.split(&ab, n)?;
            let ac = a.load_children_mut(&ab)?;
            let bc = b.load_children(&bb)?;
            remove_prefix_children_with(ac, ab, bc, bb, f)?;
        }
    } else if n == ap.len() {
        // self is a prefix of that
        let ac = a.load_children_mut(&ab)?;
        let bc = [b.clone_shortened(&bb, n)?];
        remove_prefix_children_with(ac, ab, TreeNodeIter::from_slice(&bc), bb, f)?;
    } else {
        // disjoint, nothing to do
    }
    a.canonicalize();
    Ok(())
}

fn remove_prefix_children_with<'a, A, B, F>(
    ac: &'a mut Vec<TreeNode<A>>,
    ab: A,
    bc: Option<TreeNodeIter<'a, B>>,
    bb: B,
    f: F,
) -> Result<(), A::Error>
where
    A: BlobStore + Clone,
    B: BlobStore + Clone,
    F: Fn(&ValueRef<B>) -> Result<bool, A::Error> + Copy,
    A::Error: From<B::Error>,
{
    if let Some(bc) = bc {
        if ac.is_empty() {
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
                    remove_prefix_with(ac, ab.clone(), &bc, bb.clone(), f)?;
                    // only move if the child is non-empty
                    let non_empty = !ac.is_empty();
                    acb.consume(1, non_empty);
                }
                Ordering::Greater => {
                    // the .unwrap() is safe because cmp guarantees that there is a value
                    let _ = bci.next().unwrap();
                }
            }
        }
    }
    Ok(())
}

/// Key for iteration of radixtree prefixes
///
/// This uses copy on write to allow iterating over all prefixes without allocations, provided that the keys are not stored somewhere.
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

    pub fn to_owned_blob(&self) -> OwnedBlob {
        self.0.clone().into()
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

/// Iterator over all tree values
///
/// This is more efficient than the key value pair iterator since it does not have to keep track of the keys.
pub struct ValueIter<S: BlobStore = Detached> {
    stack: Vec<TreeNodeIter<'static, S>>,
    store: S,
}

impl<S: BlobStore> ValueIter<S> {
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

    fn next0(&mut self) -> Result<Option<Value<S>>, S::Error> {
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

impl<S: BlobStore> Iterator for ValueIter<S> {
    type Item = Result<Value<S>, S::Error>;

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

/// Iterator over keys and values of a [RadixTree]
///
/// The values are constructed as the tree is traversed. Therefore iteration is slightly more expensive than
/// iterating over just values using [ValueIter]
pub struct KeyValueIter<S: BlobStore = Detached> {
    path: IterKey,
    stack: Vec<(usize, Option<TreeNodeIter<'static, S>>)>,
    store: S,
}

impl<S: BlobStore> KeyValueIter<S> {
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

    fn next0(&mut self) -> Result<Option<(IterKey, Value<S>)>, S::Error> {
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

impl<S: BlobStore> Iterator for KeyValueIter<S> {
    type Item = Result<(IterKey, Value<S>), S::Error>;

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
    stack: Vec<(usize, Option<TreeNodeIter<'static, S>>)>,
    store: S,
    descend: F,
}

impl<S: BlobStore, F: Fn(&[u8], &TreeNodeRef<S>) -> Result<bool, S::Error>> GroupBy<S, F> {
    fn new(iter: TreeNodeIter<'static, S>, store: S, prefix: IterKey, descend: F) -> Self {
        Self {
            stack: vec![(0, Some(iter))],
            path: prefix,
            store,
            descend,
        }
    }

    fn next0(&mut self) -> Result<Option<TreeNode<S>>, S::Error> {
        Ok(loop {
            if self.stack.is_empty() {
                break None;
            }
            let (last_prefix_len, iter_opt) = &mut self.stack.last_mut().unwrap();
            let last_prefix_len = *last_prefix_len;
            if let Some(iter) = iter_opt {
                if let Some(node) = iter.next() {
                    // apply the prefix in any case!
                    let prefix = node.load_prefix(&self.store)?;
                    let prefix_len = prefix.len();
                    self.path.append(prefix.as_ref());
                    if (self.descend)(&self.path.0, &node)? {
                        let children = node.load_children_owned(&self.store)?;
                        let res = if node.value_opt().is_some() {
                            let mut res = node.to_owned();
                            res.set_prefix_slice(&self.path);
                            res.set_children_arc_opt(None);
                            Some(res)
                        } else {
                            None
                        };
                        self.stack.push((prefix_len, children));
                        if let Some(t) = res {
                            break Some(t);
                        }
                    } else {
                        let mut res = node.to_owned();
                        res.set_prefix_slice(&self.path);
                        // undo applying the prefix immediately, since we don't descend
                        self.path.pop(prefix_len);
                        break Some(res);
                    }
                } else {
                    *iter_opt = None;
                }
            } else {
                self.path.pop(last_prefix_len);
                self.stack.pop();
            }
        })
    }
}

impl<S: BlobStore, F: Fn(&[u8], &TreeNodeRef<S>) -> Result<bool, S::Error>> Iterator
    for GroupBy<S, F>
{
    type Item = Result<TreeNode<S>, S::Error>;

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

impl<S: BlobStore + Default> Default for RadixTree<S> {
    fn default() -> Self {
        Self::empty(S::default())
    }
}

impl RadixTree {
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Value> {
        self.try_get(key).unwrap_safe()
    }

    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.try_contains_key(key).unwrap_safe()
    }

    pub fn has_prefix(&self, prefix: impl AsRef<[u8]>) -> bool {
        self.try_has_prefix(prefix).unwrap_safe()
    }

    pub fn remove_prefix(&mut self, prefix: impl AsRef<[u8]>) {
        self.try_remove_prefix(prefix).unwrap_safe()
    }

    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.try_insert(key, value).unwrap_safe()
    }

    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.try_remove(key).unwrap_safe()
    }

    pub fn iter(&self) -> impl Iterator<Item = (IterKey, Value)> {
        self.try_iter().map(|x| x.unwrap_safe())
    }

    pub fn values(&self) -> impl Iterator<Item = Value> {
        self.try_values().map(|x| x.unwrap_safe())
    }

    pub fn scan_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl Iterator<Item = (IterKey, Value)> + '_ {
        self.try_scan_prefix(prefix)
            .unwrap_safe()
            .map(|x| x.unwrap_safe())
    }

    pub fn group_by<'a>(
        &'a self,
        f: impl Fn(&[u8], &TreeNodeRef) -> bool + 'a,
    ) -> impl Iterator<Item = RadixTree> + 'a {
        self.try_group_by(move |a, b| Ok(f(a, b)))
            .map(|x| x.unwrap_safe())
    }

    pub fn dump(&self) {
        self.try_dump().unwrap_safe()
    }

    pub fn outer_combine(
        &self,
        that: &RadixTree,
        f: impl Fn(&ValueRef, &ValueRef) -> Option<Value> + Copy,
    ) -> RadixTree {
        self.try_outer_combine(that, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn inner_combine(
        &self,
        that: &RadixTree,
        f: impl Fn(&ValueRef, &ValueRef) -> Option<Value> + Copy,
    ) -> RadixTree {
        self.try_inner_combine(that, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn inner_combine_pred(
        &self,
        that: &RadixTree,
        f: impl Fn(&ValueRef, &ValueRef) -> bool + Copy,
    ) -> bool {
        self.try_inner_combine_pred(that, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn left_combine(
        &self,
        that: &RadixTree,
        f: impl Fn(&ValueRef, &ValueRef) -> Option<Value> + Copy,
    ) -> RadixTree {
        self.try_left_combine(that, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn left_combine_pred(
        &self,
        that: &RadixTree,
        f: impl Fn(&ValueRef, &ValueRef) -> bool + Copy,
    ) -> bool {
        self.try_left_combine_pred(that, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn first_value(&self) -> Option<Value> {
        self.try_first_value().unwrap_safe()
    }

    pub fn last_value(&self) -> Option<Value> {
        self.try_last_value().unwrap_safe()
    }

    pub fn first_entry(&self, prefix: Vec<u8>) -> Option<(Vec<u8>, Value)> {
        self.try_first_entry(prefix).unwrap_safe()
    }

    pub fn last_entry(&self, prefix: Vec<u8>) -> Option<(Vec<u8>, Value)> {
        self.try_last_entry(prefix).unwrap_safe()
    }
}

impl RadixTree {
    pub fn leaf(value: impl AsRef<[u8]>) -> Self {
        Self::new(TreeNode::leaf(value.as_ref()), Detached)
    }

    pub fn single(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self::new(TreeNode::single(key.as_ref(), value.as_ref()), Detached)
    }

    pub fn outer_combine_with<S2: BlobStore<Error = NoError> + Clone>(
        &mut self,
        that: &RadixTree<S2>,
        f: impl Fn(&mut Value, &ValueRef<S2>) + Copy,
    ) {
        self.try_outer_combine_with(that, DetachConverter, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn inner_combine_with<S2: BlobStore<Error = NoError> + Clone>(
        &mut self,
        that: &RadixTree<S2>,
        f: impl Fn(&mut Value, &ValueRef<S2>) + Copy,
    ) {
        self.try_inner_combine_with(that, DetachConverter, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn left_combine_with<S2: BlobStore<Error = NoError> + Clone>(
        &mut self,
        that: &RadixTree<S2>,
        f: impl Fn(&mut Value, &ValueRef<S2>) + Copy,
    ) {
        self.try_left_combine_with(that, DetachConverter, |a, b| Ok(f(a, b)))
            .unwrap_safe()
    }

    pub fn filter_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
        substitution: impl AsRef<[u8]>,
    ) -> RadixTree {
        self.try_filter_prefix(prefix, substitution).unwrap_safe()
    }

    pub fn retain_prefix_with<S2: BlobStore<Error = NoError> + Clone>(
        &mut self,
        that: &RadixTree<S2>,
        f: impl Fn(&ValueRef<S2>) -> bool + Copy,
    ) {
        self.try_retain_prefix_with(that, |a| Ok(f(a)))
            .unwrap_safe()
    }

    pub fn remove_prefix_with<S2: BlobStore<Error = NoError> + Clone>(
        &mut self,
        that: &RadixTree<S2>,
        f: impl Fn(&ValueRef<S2>) -> bool + Copy,
    ) {
        self.try_remove_prefix_with(that, |a| Ok(f(a)))
            .unwrap_safe()
    }
}

impl RadixTree {
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_attached<S: BlobStore>(&self, store: S) -> Result<RadixTree<S>, S::Error> {
        let node = self.node.try_attached(&store)?;
        Ok(RadixTree { node, store })
    }
}

impl<S: BlobStore> RadixTree<S> {
    pub fn empty(store: S) -> Self {
        Self::new(TreeNode::<S>::EMPTY, store)
    }

    pub fn try_load(store: S, id: Option<impl AsRef<[u8]>>) -> Result<Self, S::Error> {
        Ok(if let Some(id) = id {
            let data = store.read(id.as_ref())?;
            let node = TreeNode::deserialize(&data)?;
            Self::new(node, store)
        } else {
            Self::empty(store)
        })
    }

    fn new(node: TreeNode<S>, store: S) -> Self {
        Self { node, store }
    }

    pub fn store(this: &Self) -> &S {
        &this.store
    }

    pub fn is_empty(&self) -> bool {
        self.node.is_empty()
    }

    pub fn is_leaf(&self) -> bool {
        self.node.is_leaf()
    }

    pub fn value(&self) -> Option<ValueRef<S>> {
        let r = self.node.value_ref();
        if !r.is_none() {
            Some(ValueRef(Ok(r), PhantomData))
        } else {
            None
        }
    }

    pub fn prefix(&self) -> ValueRef<S> {
        let r = self.node.prefix_ref();
        ValueRef(Ok(r), PhantomData)
    }
}

impl PartialEq for RadixTree {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl Eq for RadixTree {}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> FromIterator<(K, V)> for RadixTree {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut tree = RadixTree::default();
        for (key, value) in iter.into_iter() {
            tree.insert(key, value);
        }
        tree
    }
}

impl<S: BlobStore + Clone> RadixTree<S> {
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_dump(&self) -> Result<(), S::Error> {
        self.node.dump(0, &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_detached(&self) -> Result<RadixTree, S::Error> {
        let node = self.node.detached(&self.store)?;
        Ok(RadixTree {
            node,
            store: Detached,
        })
    }

    /// Get the value for a given key
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_get(&self, key: impl AsRef<[u8]>) -> Result<Option<Value<S>>, S::Error> {
        self.node.get(key.as_ref(), &self.store)
    }

    /// True if key is contained in this set
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool, S::Error> {
        self.node.contains_key(key.as_ref(), &self.store)
    }

    /// True if there are keys at or below the given prefix
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_has_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<bool, S::Error> {
        self.node.has_prefix(prefix.as_ref(), &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_is_prefix(&self, key: impl AsRef<[u8]>) -> Result<bool, S::Error> {
        self.node.is_prefix(key.as_ref(), &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_remove_prefix(&mut self, prefix: impl AsRef<[u8]>) -> Result<(), S::Error> {
        self.try_remove_prefix_with(&RadixTree::single(prefix, &[]), |_| Ok(true))
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), S::Error> {
        self.try_outer_combine_with(
            &RadixTree::single(key, value),
            DowncastConverter,
            |value, replacement| {
                value.set(Some(replacement.downcast()));
                Ok(())
            },
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_remove(&mut self, key: impl AsRef<[u8]>) -> Result<(), S::Error> {
        self.try_left_combine_with(&RadixTree::single(key, &[]), DowncastConverter, |l, _| {
            l.set(None);
            Ok(())
        })
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_iter(&self) -> KeyValueIter<S> {
        KeyValueIter::new(
            TreeNodeIter::from_arc(Arc::new(vec![self.node.clone()])),
            self.store.clone(),
            IterKey::default(),
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_values(&self) -> ValueIter<S> {
        ValueIter::new(
            TreeNodeIter::from_arc(Arc::new(vec![self.node.clone()])),
            self.store.clone(),
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_scan_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<KeyValueIter<S>, S::Error> {
        scan_prefix(
            self.store.clone(),
            &TreeNodeRef::owned(&self.node),
            prefix.as_ref(),
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_group_by<'a, F: Fn(&[u8], &TreeNodeRef<S>) -> Result<bool, S::Error> + 'a>(
        &'a self,
        descend: F,
    ) -> impl Iterator<Item = Result<RadixTree<S>, S::Error>> + 'a {
        GroupBy::new(
            TreeNodeIter::from_arc(Arc::new(vec![self.node.clone()])),
            self.store.clone(),
            IterKey::default(),
            descend,
        )
        .map(|r| {
            r.map(|node| RadixTree {
                node,
                store: self.store.clone(),
            })
        })
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_outer_combine<S2, E, F>(&self, that: &RadixTree<S2>, f: F) -> Result<RadixTree, E>
    where
        S2: BlobStore + Clone,
        E: From<S2::Error> + From<S::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<Option<Value>, E> + Copy,
    {
        Ok(RadixTree {
            node: outer_combine(
                &TreeNodeRef::owned(&self.node),
                self.store.clone(),
                &TreeNodeRef::owned(&that.node),
                that.store.clone(),
                f,
            )?,
            store: Detached,
        })
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_outer_combine_with<S2, C, F>(
        &mut self,
        that: &RadixTree<S2>,
        c: C,
        f: F,
    ) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        C: NodeConverter<S2, S> + Clone,
        F: Fn(&mut Value<S>, &ValueRef<S2>) -> Result<(), S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        outer_combine_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            c,
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_inner_combine<S2, E, F>(&self, that: &RadixTree<S2>, f: F) -> Result<RadixTree, E>
    where
        S2: BlobStore + Clone,
        E: From<S2::Error> + From<S::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<Option<Value>, E> + Copy,
    {
        Ok(RadixTree {
            node: inner_combine(
                &TreeNodeRef::owned(&self.node),
                self.store.clone(),
                &TreeNodeRef::owned(&that.node),
                that.store.clone(),
                f,
            )?,
            store: Detached,
        })
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_inner_combine_with<S2, C, F>(
        &mut self,
        that: &RadixTree<S2>,
        c: C,
        f: F,
    ) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        C: NodeConverter<S2, S> + Clone,
        F: Fn(&mut Value<S>, &ValueRef<S2>) -> Result<(), S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        inner_combine_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            c,
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_inner_combine_pred<S2, E, F>(&self, that: &RadixTree<S2>, f: F) -> Result<bool, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<bool, E> + Copy,
    {
        inner_combine_pred(
            &TreeNodeRef::owned(&self.node),
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_left_combine<S2, E, F>(&self, that: &RadixTree<S2>, f: F) -> Result<RadixTree, E>
    where
        S2: BlobStore + Clone,
        E: From<S2::Error> + From<S::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<Option<Value>, E> + Copy,
    {
        Ok(RadixTree {
            node: left_combine(
                &TreeNodeRef::owned(&self.node),
                self.store.clone(),
                &TreeNodeRef::owned(&that.node),
                that.store.clone(),
                f,
            )?,
            store: Detached,
        })
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_left_combine_pred<S2, E, F>(&self, that: &RadixTree<S2>, f: F) -> Result<bool, E>
    where
        S2: BlobStore + Clone,
        E: From<S::Error> + From<S2::Error>,
        F: Fn(&ValueRef<S>, &ValueRef<S2>) -> Result<bool, E> + Copy,
    {
        left_combine_pred(
            &TreeNodeRef::owned(&self.node),
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_left_combine_with<S2, C, F>(
        &mut self,
        that: &RadixTree<S2>,
        c: C,
        f: F,
    ) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        C: NodeConverter<S2, S> + Clone,
        F: Fn(&mut Value<S>, &ValueRef<S2>) -> Result<(), S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        left_combine_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            c,
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_retain_prefix_with<S2, F>(&mut self, that: &RadixTree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        F: Fn(&ValueRef<S2>) -> Result<bool, S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        retain_prefix_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_remove_prefix_with<S2, F>(&mut self, that: &RadixTree<S2>, f: F) -> Result<(), S::Error>
    where
        S2: BlobStore + Clone,
        F: Fn(&ValueRef<S2>) -> Result<bool, S::Error> + Copy,
        S::Error: From<S2::Error> + From<NoError>,
    {
        remove_prefix_with(
            &mut self.node,
            self.store.clone(),
            &TreeNodeRef::owned(&that.node),
            that.store.clone(),
            f,
        )
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_filter_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
        substitution: impl AsRef<[u8]>,
    ) -> Result<RadixTree<S>, S::Error> {
        filter_prefix(
            &TreeNodeRef::owned(&self.node),
            &self.store,
            prefix.as_ref(),
            substitution.as_ref(),
        )
        .map(|node| RadixTree::new(node, self.store.clone()))
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_first_value(&self) -> Result<Option<Value<S>>, S::Error> {
        first_value(&TreeNodeRef::owned(&self.node), &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_last_value(&self) -> Result<Option<Value<S>>, S::Error> {
        last_value(&TreeNodeRef::owned(&self.node), &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_first_entry(&self, prefix: Vec<u8>) -> Result<Option<(Vec<u8>, Value<S>)>, S::Error> {
        first_entry(prefix, &TreeNodeRef::owned(&self.node), &self.store)
    }

    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_last_entry(&self, prefix: Vec<u8>) -> Result<Option<(Vec<u8>, Value<S>)>, S::Error> {
        last_entry(prefix, &TreeNodeRef::owned(&self.node), &self.store)
    }

    /// Writes the entire tree to the store
    #[cfg_attr(feature = "custom-store", visibility::make(pub))]
    fn try_reattach(&mut self) -> Result<Vec<u8>, S::Error> {
        let mut data = Vec::new();
        self.node.serialize(&mut data, &self.store)?;
        let id = self.store.write(&data)?;
        self.node = TreeNode::deserialize(&data)?;
        Ok(id)
    }
}
