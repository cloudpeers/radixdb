use anyhow::Context;
use binary_merge::{MergeOperation, MergeState};
use std::{borrow::Borrow, cmp::Ordering, fmt::Debug, sync::Arc};

use crate::{
    blob_store::NoStore,
    flex_ref::FlexRef,
    merge_state::{DynT, MergeStateMut, NoStoreT, VecMergeState, TT, TTI},
};

use super::*;

#[repr(transparent)]
#[derive(Clone)]
pub struct TreeValue(FlexRef<Vec<u8>>);

/// A tree prefix is an optional blob, that is stored either inline, on the heap, or as an id
impl TreeValue {
    fn none() -> Self {
        Self(FlexRef::none())
    }

    fn is_none(&self) -> bool {
        self.0.is_none()
    }

    fn is_some(&self) -> bool {
        !self.is_none()
    }

    fn from_slice(data: &[u8]) -> Self {
        Self(FlexRef::inline_or_owned_from_slice(data))
    }

    pub fn load<S: BlobStore>(&self, store: &S) -> std::result::Result<Option<Blob<u8>>, S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            Ok(Some(Blob::arc_vec_t(arc.clone())))
        } else if let Some(data) = self.0.inline_as_ref() {
            Ok(Some(Blob::inline(data).unwrap()))
        } else if let Some(id) = self.0.id_u64() {
            store.read(id).map(Some)
        } else if self.0.is_none() {
            Ok(None)
        } else {
            unreachable!("invalid state of a TreePrefix");
        }
    }

    fn validate_serialized(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.0.is_inline() || self.0.is_id() || self.0.is_none());
        Ok(())
    }

    /// detaches the value from the store. on success it will either be none, inline, or owned
    fn detach<S: BlobStore>(&mut self, store: &S) -> std::result::Result<(), S::Error> {
        if let Some(id) = self.0.id_u64() {
            let slice = store.read(id)?;
            self.0 = FlexRef::inline_or_owned_from_slice(slice.as_ref());
        }
        Ok(())
    }

    fn detached<S: BlobStore>(&self, store: &S) -> std::result::Result<Self, S::Error> {
        let mut t = self.clone();
        t.detach(store)?;
        Ok(t)
    }

    /// attaches the value to the store. on success it will either be none, inline or id
    ///
    /// if the value is already attached, it is assumed that it is to the store, so it is a noop
    fn attach<S: BlobStore>(&mut self, store: &mut S) -> std::result::Result<(), S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            let data = arc.as_ref();
            let first = data.get(0).cloned();
            let id = store.write(data)?;
            self.0 = FlexRef::id_from_u64_and_extra(id, first).unwrap();
        }
        Ok(())
    }
}

impl Debug for TreeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(data) = self.0.inline_as_ref() {
            f.debug_tuple("Inline").field(&Hex::new(data)).finish()
        } else if let Some(arc) = self.0.owned_arc_ref() {
            f.debug_tuple("Owned")
                .field(&Hex::partial(arc.as_ref(), 32))
                .finish()
        } else if let Some(id) = self.0.id_u64() {
            f.debug_tuple("Id").field(&id).finish()
        } else if self.0.is_none() {
            f.debug_tuple("None").finish()
        } else {
            f.debug_tuple("Invalid").finish()
        }
    }
}

impl Default for TreeValue {
    fn default() -> Self {
        Self::none()
    }
}

/// A tree prefix is a blob, that is stored either inline, on the heap, or as an id
#[repr(transparent)]
#[derive(Clone)]
pub struct TreePrefix(FlexRef<Vec<u8>>);

impl TreePrefix {
    fn validate_serialized(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.0.is_inline() || self.0.is_id());
        Ok(())
    }

    fn from_slice(data: &[u8]) -> Self {
        Self(FlexRef::inline_or_owned_from_slice(data))
    }

    fn from_vec(data: Vec<u8>) -> Self {
        Self(FlexRef::inline_or_owned_from_vec(data))
    }

    fn from_arc_vec(data: Arc<Vec<u8>>) -> Self {
        Self(FlexRef::owned_from_arc(data))
    }

    fn truncate<S: BlobStore>(
        &mut self,
        n: usize,
        store: &S,
    ) -> std::result::Result<TreePrefix, S::Error> {
        let mut r;
        if self.load(store)?.as_ref().len() <= n {
            r = TreePrefix::empty();
        } else if n == 0 {
            r = TreePrefix::empty();
            std::mem::swap(&mut r, self);
        } else {
            let vec = self.make_mut(store)?;
            r = TreePrefix::from_slice(&vec[n..]);
            vec.truncate(n);
        }
        Ok(r)
    }

    fn append<S: BlobStore>(
        &mut self,
        that: &TreePrefix,
        store: &S,
    ) -> std::result::Result<(), S::Error> {
        if !that.is_empty() {
            if self.is_empty() {
                *self = that.clone();
            } else {
                let a = self.load(store)?;
                let b = that.load(store)?;
                if a.len() + b.len() <= 6 {
                    let mut c = a.to_vec();
                    c.extend_from_slice(b.as_ref());
                    self.0 = FlexRef::inline_from_slice(&c).unwrap();
                } else {
                    self.make_mut(store)?.extend_from_slice(b.as_ref());
                }
            }
        };
        Ok(())
    }

    pub fn load<S: BlobStore>(&self, store: &S) -> std::result::Result<Blob<u8>, S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            Ok(Blob::arc_vec_t(arc.clone()))
        } else if let Some(slice) = self.0.inline_as_ref() {
            Ok(Blob::inline(slice).unwrap())
        } else if let Some(id) = self.0.id_u64() {
            store.read(id).map(|x| x)
        } else {
            unreachable!("invalid state of a TreePrefix");
        }
    }

    fn slice_opt(&self) -> Option<&[u8]> {
        if let Some(data) = self.0.inline_as_ref() {
            Some(data)
        } else if let Some(arc) = self.0.owned_arc_ref() {
            Some(arc.as_ref())
        } else {
            None
        }
    }

    /// detaches the prefix from the store. on success it will either be inline or owned
    fn detach<S: BlobStore>(&mut self, store: &S) -> std::result::Result<(), S::Error> {
        if let Some(id) = self.0.id_u64() {
            let slice = store.read(id)?;
            self.0 = FlexRef::inline_or_owned_from_slice(slice.as_ref());
        }
        Ok(())
    }

    fn detached<S: BlobStore>(&self, store: &S) -> std::result::Result<Self, S::Error> {
        let mut t = self.clone();
        t.detach(store)?;
        Ok(t)
    }

    /// attaches the prefix to the store. on success it will either be inline or id
    ///
    /// if the prefix is already attached, it is assumed that it is to the store, so it is a noop
    fn attach<S: BlobStore>(&mut self, store: &mut S) -> std::result::Result<(), S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            let data = arc.as_ref();
            let first = data.get(0).cloned();
            let id = store.write(data)?;
            self.0 = FlexRef::id_from_u64_and_extra(id, first).unwrap();
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        if let Some(arc) = self.0.owned_arc_ref() {
            arc.as_ref().is_empty()
        } else {
            self.0 == FlexRef::inline_empty_array()
        }
    }

    fn empty() -> Self {
        Self(FlexRef::inline_empty_array())
    }

    fn first_opt(&self) -> Option<u8> {
        if let Some(data) = self.0.inline_as_ref() {
            data.get(0).cloned()
        } else if let Some(arc) = self.0.owned_arc_ref() {
            arc.get(0).cloned()
        } else if let Some(first) = self.0.id_extra_data() {
            first
        } else {
            None
        }
    }

    fn make_mut<S: BlobStore>(&mut self, store: &S) -> std::result::Result<&mut Vec<u8>, S::Error> {
        if !self.0.is_arc() {
            let data = self.load(store)?;
            self.0 = FlexRef::owned_from_arc(Arc::new(data.as_ref().to_vec()));
        }
        let arc = self.0.owned_arc_ref_mut().expect("must be an arc");
        Ok(Arc::make_mut(arc))
    }

    fn join(a: &[u8], b: &[u8]) -> Self {
        let mut t = Vec::with_capacity(a.len() + b.len());
        t.extend_from_slice(a);
        t.extend_from_slice(b);
        Self(FlexRef::inline_or_owned_from_vec(t))
    }
}

impl Debug for TreePrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(data) = self.0.inline_as_ref() {
            f.debug_tuple("Inline").field(&Hex::new(data)).finish()
        } else if let Some(arc) = self.0.owned_arc_ref() {
            f.debug_tuple("Owned")
                .field(&Hex::partial(arc.as_ref(), 32))
                .finish()
        } else if let Some(id) = self.0.id_u64() {
            f.debug_tuple("Id").field(&id).finish()
        } else {
            f.debug_tuple("Invalid").finish()
        }
    }
}

impl Default for TreePrefix {
    fn default() -> Self {
        Self::empty()
    }
}

/// Tree children are an optional array, that are stored either on the heap, or as an id
#[repr(transparent)]
#[derive(Clone)]
pub struct TreeChildren(FlexRef<Vec<TreeNode>>);

impl TreeChildren {
    fn from_arc(data: Arc<Vec<TreeNode>>) -> Self {
        Self(FlexRef::owned_from_arc(data))
    }

    fn from_vec(vec: Vec<TreeNode>) -> Self {
        if vec.is_empty() {
            Self::default()
        } else {
            Self::from_arc(Arc::new(vec))
        }
    }

    fn from_slice(slice: &[TreeNode]) -> Self {
        if slice.is_empty() {
            Self::default()
        } else {
            Self::from_vec(slice.to_vec())
        }
    }

    fn empty() -> Self {
        Self(FlexRef::none())
    }

    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn load<S: BlobStore>(&self, store: &S) -> std::result::Result<Blob<TreeNode>, S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            Ok(Blob::arc_vec_t(arc.clone()))
        } else if let Some(id) = self.0.id_u64() {
            store.read(id).map(|x| x.cast::<TreeNode>().unwrap())
        } else if self.0.is_none() {
            Ok(Blob::inline(&[]).unwrap())
        } else {
            unreachable!("invalid state of a TreePrefix");
        }
    }

    fn validate_serialized(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.0.is_id() || self.0.is_none());
        Ok(())
    }

    fn make_mut(&mut self) -> Option<&mut Vec<TreeNode>> {
        if let Some(arc) = self.0.owned_arc_ref_mut() {
            Some(Arc::make_mut(arc))
        } else {
            None
        }
    }

    fn detach<S: BlobStore>(&mut self, store: &S) -> std::result::Result<(), S::Error> {
        if let Some(id) = self.0.id_u64() {
            let mut children = TreeNode::nodes_from_bytes(store.read(id)?.as_ref())
                .unwrap()
                .to_vec();
            for child in &mut children {
                child.detach(store)?;
            }
            self.0 = FlexRef::owned_from_arc(Arc::new(children))
        }
        Ok(())
    }

    /// attaches the children to the store. on success it be an id
    fn attach<S: BlobStore>(&mut self, store: &mut S) -> std::result::Result<(), S::Error> {
        if let Some(arc) = self.0.owned_arc_ref() {
            let mut children = arc.as_ref().clone();
            for child in &mut children {
                child.attach(store)?;
            }
            let bytes = TreeNode::slice_to_bytes(&children).unwrap();
            let first = bytes.get(0).cloned();
            self.0 = FlexRef::id_from_u64_and_extra(store.write(&bytes)?, first).unwrap();
        }
        Ok(())
    }

    fn detached<S: BlobStore>(
        &self,
        store: &S,
        recursive: bool,
    ) -> std::result::Result<Self, S::Error> {
        let mut t = self.clone();
        t.detach(store)?;
        Ok(t)
    }
}

impl Debug for TreeChildren {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(arc) = self.0.owned_arc_ref() {
            let data = arc.as_ref();
            write!(f, "Owned(len={})", data.len())
        } else if let Some(id) = self.0.id_u64() {
            f.debug_tuple("Id").field(&id).finish()
        } else if self.0.is_none() {
            f.debug_tuple("None").finish()
        } else {
            f.debug_tuple("Invalid").finish()
        }
    }
}

impl Default for TreeChildren {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Default, Clone, Debug)]
#[repr(C)]
pub struct TreeNode {
    prefix: TreePrefix,
    value: TreeValue,
    children: TreeChildren,
}

impl TreeNode {
    fn validate_serialized(&self) -> anyhow::Result<()> {
        self.prefix.validate_serialized()?;
        self.value.validate_serialized()?;
        self.children.validate_serialized()?;
        Ok(())
    }

    pub fn empty() -> Self {
        Self {
            prefix: TreePrefix::empty(),
            value: TreeValue::none(),
            children: TreeChildren::empty(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && self.value.is_none()
    }

    fn nodes_from_bytes(bytes: &[u8]) -> anyhow::Result<&[TreeNode]> {
        let res: &[TreeNode] = slice_cast(bytes)?;
        for node in res {
            node.validate_serialized()?;
        }
        Ok(res)
    }

    fn slice_to_bytes(nodes: &[Self]) -> anyhow::Result<Vec<u8>> {
        let mut res = Vec::with_capacity(nodes.len() * 24);
        for node in nodes {
            anyhow::ensure!(!node.prefix.0.is_arc());
            anyhow::ensure!(!node.value.0.is_arc());
            anyhow::ensure!(!node.children.0.is_arc());
            res.extend_from_slice(&node.prefix.0 .0);
            res.extend_from_slice(&node.value.0 .0);
            res.extend_from_slice(&node.children.0 .0);
        }
        Ok(res)
    }

    pub fn leaf(value: &[u8]) -> Self {
        Self {
            value: TreeValue::from_slice(value),
            ..Default::default()
        }
    }

    pub fn single(key: &[u8], value: &[u8]) -> Self {
        Self {
            prefix: TreePrefix::from_slice(key),
            value: TreeValue::from_slice(value),
            ..Default::default()
        }
    }

    pub fn new(key: &[u8], value: Option<&[u8]>, children: &[Self]) -> Self {
        Self {
            prefix: TreePrefix::from_slice(key),
            value: value.map(TreeValue::from_slice).unwrap_or_default(),
            children: TreeChildren::from_vec(children.to_vec()),
        }
    }

    /// detach the node from its `store`.
    pub fn detach<S: BlobStore>(&mut self, store: &S) -> std::result::Result<(), S::Error> {
        self.prefix.detach(store)?;
        self.value.detach(store)?;
        self.children.detach(store)?;
        Ok(())
    }

    /// Return the subtree with the given prefix. Will return an empty tree in case there is no match.
    pub fn filter_prefix<S: BlobStore>(
        &self,
        store: &S,
        prefix: &[u8],
    ) -> std::result::Result<Self, S::Error> {
        Ok(match find(store, self, prefix)? {
            FindResult::Found(mut res) => {
                res.prefix = TreePrefix(FlexRef::inline_or_owned_from_slice(prefix));
                res
            }
            FindResult::Prefix { tree: mut res, rt } => {
                let rp = res.prefix.load(store)?;
                res.prefix = TreePrefix::join(prefix, &rp[rp.len() - rt..]);
                res
            }
            FindResult::NotFound { .. } => Self::default(),
        })
    }

    /// Prepend a prefix to the tree
    pub fn prepend<S: BlobStore>(
        &mut self,
        prefix: &[u8],
        store: &S,
    ) -> std::result::Result<(), S::Error> {
        let mut t = TreePrefix(FlexRef::inline_or_owned_from_slice(prefix));
        t.append(&self.prefix, store)?;
        self.prefix = t;
        Ok(())
    }

    /// True if key is contained in this set
    pub fn contains_key<S: BlobStore>(
        &self,
        store: &S,
        key: &[u8],
    ) -> std::result::Result<bool, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        Ok(if let FindResult::Found(tree) = find(store, self, key)? {
            tree.value.is_some()
        } else {
            false
        })
    }

    /// Get the value for a given key
    pub fn get<S: BlobStore>(
        &self,
        store: &S,
        key: &[u8],
    ) -> std::result::Result<Option<Blob<u8>>, S::Error> {
        // if we find a tree at exactly the location, and it has a value, we have a hit
        Ok(if let FindResult::Found(tree) = find(store, self, key)? {
            tree.value.load(store)?
        } else {
            None
        })
    }

    pub fn insert<S: BlobStore>(
        &mut self,
        store: &S,
        key: &[u8],
        value: &[u8],
    ) -> std::result::Result<(), S::Error> {
        *self = outer_combine(
            TTI::<S, NoStore, S::Error>::new(),
            self,
            store,
            &TreeNode::single(key, value),
            &NoStore,
            |_, b| Ok(b),
        )?;
        Ok(())
    }

    pub fn remove<S: BlobStore>(
        &mut self,
        store: &S,
        key: &[u8],
    ) -> std::result::Result<(), S::Error> {
        *self = left_combine(
            TTI::<S, NoStore, S::Error>::new(),
            self,
            store,
            &TreeNode::single(key, &[]),
            &NoStore,
            |_, _| Ok(TreeValue::none()),
        )?;
        Ok(())
    }

    /// An iterator for all pairs with a certain prefix
    pub fn scan_prefix<'a, S: BlobStore>(
        &self,
        store: &'a S,
        prefix: &[u8],
    ) -> std::result::Result<Iter<'a, S>, S::Error> {
        Ok(match find(store, self, prefix)? {
            FindResult::Found(tree) => {
                let prefix = IterKey::new(prefix);
                Iter::new(tree, store, prefix)
            }
            FindResult::Prefix { tree, rt } => {
                let tree_prefix = tree.prefix.load(store)?;
                let mut prefix = IterKey::new(prefix);
                let remaining = &tree_prefix.as_ref()[tree_prefix.len() - rt..];
                prefix.append(remaining);
                Iter::new(tree, store, prefix)
            }
            FindResult::NotFound { .. } => Iter::empty(store),
        })
    }

    /// detach the node from its `store`.
    ///
    /// if `recursive` is true, the tree gets completely detached, otherwise just one level deep.
    pub fn detached<S: BlobStore>(&self, store: &S) -> std::result::Result<Self, S::Error> {
        let mut t = self.clone();
        t.detach(store)?;
        Ok(t)
    }

    /// attaches the node components to the store
    pub fn attach<S: BlobStore>(&mut self, store: &mut S) -> std::result::Result<(), S::Error> {
        self.prefix.attach(store)?;
        self.value.attach(store)?;
        self.children.attach(store)?;
        Ok(())
    }

    pub(crate) fn clone_shortened<S: BlobStore>(
        &self,
        store: &S,
        n: usize,
    ) -> std::result::Result<Self, S::Error> {
        let prefix = self.prefix.load(store)?;
        assert!(n < prefix.len());
        Ok(Self {
            prefix: TreePrefix::from_slice(&prefix[n..]),
            value: self.value.clone(),
            children: self.children.clone(),
        })
    }

    pub(crate) fn split<S: BlobStore>(
        &mut self,
        store: &S,
        n: usize,
    ) -> std::result::Result<(), S::Error> {
        let rest = self.prefix.truncate(n, store)?;
        let mut child = Self {
            value: TreeValue::none(),
            children: TreeChildren::empty(),
            prefix: rest,
        };
        // child.childen = self.children, self.children = empty
        std::mem::swap(&mut self.children, &mut child.children);
        // child.value = self.value, self.value = none
        std::mem::swap(&mut self.value, &mut child.value);
        // now, self is a degenerate empty node with first being the prefix
        // child is the former self (value and children) with rest as prefix
        self.children = TreeChildren::from_vec(vec![child]);
        Ok(())
    }

    pub(crate) fn unsplit<S: BlobStore>(&mut self, store: &S) -> std::result::Result<(), S::Error> {
        // remove all empty children
        // this might sometimes not be necessary, but it is tricky to find out when.
        if !self.children.is_empty() {
            if let Some(children) = self.children.make_mut() {
                children.retain(|x| !x.is_empty());
                // a single child and no own value is degenerate
                if children.len() == 1 && self.value.is_none() {
                    let child = children.pop().unwrap();
                    self.prefix.append(&child.prefix, store)?;
                    self.children = child.children;
                    self.value = child.value;
                } else if children.len() == 0 {
                    // no children left - canonicalize to empty
                    self.children = TreeChildren::empty();
                }
            }
        }
        if self.is_empty() {
            // canonicalize prefix
            self.prefix = TreePrefix::empty();
        }
        Ok(())
    }

    pub fn dump_tree(&self, store: &DynBlobStore) -> anyhow::Result<()> {
        fn hex(x: &[u8]) -> anyhow::Result<String> {
            Ok(hex::encode(x))
        }
        self.dump_tree_custom("", store, hex, |_, v| hex(v), |_| Ok("".into()))
    }

    pub fn dump_tree_utf8(&self, store: &DynBlobStore) -> anyhow::Result<()> {
        fn utf8(x: &[u8]) -> anyhow::Result<String> {
            Ok(std::str::from_utf8(x)?.to_owned())
        }
        self.dump_tree_custom("", store, utf8, |_, v| utf8(v), |_| Ok("".into()))
    }

    pub fn dump_tree_custom(
        &self,
        indent: &str,
        store: &DynBlobStore,
        p: impl Fn(&[u8]) -> anyhow::Result<String> + Copy,
        v: impl Fn(&[u8], &[u8]) -> anyhow::Result<String> + Copy,
        n: impl Fn(&Self) -> anyhow::Result<String> + Copy,
    ) -> anyhow::Result<()> {
        let prefix = self.prefix.load(store)?;
        let value = self.value.load(store)?;
        let children = self.children.load(store)?;
        let formatted_prefix = p(prefix.as_ref())?;
        if let Some(value) = value {
            println!(
                "{}{}:{}\t{}",
                indent,
                formatted_prefix,
                v(prefix.as_ref(), value.as_ref())?,
                n(self)?,
            );
        } else {
            println!("{}{}\t{}", indent, formatted_prefix, n(self)?);
        }
        let indent = indent.to_owned() + &formatted_prefix;
        for child in children.as_ref() {
            child.dump_tree_custom(&indent, store, p, v, n)?;
        }
        Ok(())
    }

    pub fn try_group_by<'a, F: Fn(&[u8], &TreeNode) -> bool>(
        &'a self,
        store: &'a DynBlobStore,
        descend: F,
    ) -> anyhow::Result<GroupBy<'a, F>> {
        let prefix = self.prefix.load(store)?;
        Ok(GroupBy::new(
            self,
            store,
            IterKey::new(prefix.as_ref()),
            descend,
        ))
    }

    /// iterate over all elements
    pub fn try_iter<'a, S: BlobStore>(
        &self,
        store: &'a S,
    ) -> std::result::Result<Iter<'a, S>, S::Error> {
        let prefix = self.prefix.load(store)?;
        Ok(Iter::new(
            self.clone(),
            store,
            IterKey::new(prefix.as_ref()),
        ))
    }

    /// iterate over all values - this is cheaper than iterating over elements, since it does not have to build the keys from fragments
    pub fn try_values<'a>(&self, store: &'a DynBlobStore) -> Values<'a> {
        Values::new(self.clone(), store)
    }

    /// get the first value
    pub fn first_value(&self, store: &DynBlobStore) -> anyhow::Result<Option<TreeValue>> {
        Ok(if self.children.is_empty() {
            if self.value.is_none() {
                None
            } else {
                Some(self.value.clone())
            }
        } else {
            let children = self.children.load(store)?;
            children[0].first_value(store)?
        })
    }

    /// get the last value
    pub fn last_value(&self, store: &DynBlobStore) -> anyhow::Result<Option<TreeValue>> {
        Ok(if self.children.is_empty() {
            if self.value.is_none() {
                None
            } else {
                Some(self.value.clone())
            }
        } else {
            let children = self.children.load(store)?;
            children[children.len() - 1].last_value(store)?
        })
    }

    /// get the first entry
    pub fn first_entry(
        &self,
        store: &DynBlobStore,
        mut prefix: TreePrefix,
    ) -> anyhow::Result<Option<(TreePrefix, TreeValue)>> {
        prefix.append(&self.prefix, store)?;
        Ok(if self.children.is_empty() {
            if self.value.is_none() {
                // huh?
                None
            } else {
                Some((prefix, self.value.clone()))
            }
        } else {
            let children = self.children.load(store)?;
            children[0].first_entry(store, prefix)?
        })
    }

    /// get the last entry
    pub fn last_entry(
        &self,
        store: &DynBlobStore,
        mut prefix: TreePrefix,
    ) -> anyhow::Result<Option<(TreePrefix, TreeValue)>> {
        prefix.append(&self.prefix, store)?;
        Ok(if self.children.is_empty() {
            if self.value.is_none() {
                // huh?
                None
            } else {
                Some((prefix, self.value.clone()))
            }
        } else {
            let children = self.children.load(store)?;
            children[children.len() - 1].last_entry(store, prefix)?
        })
    }

    pub fn inner_combine(
        &self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<Self> {
        inner_combine(DynT, self, store, that, that_store, f)
    }

    pub fn inner_combine_with(
        &mut self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<()> {
        *self = inner_combine(DynT, self, store, that, that_store, f)?;
        Ok(())
    }

    pub fn outer_combine(
        &self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<Self> {
        outer_combine(DynT, self, store, that, that_store, f)
    }

    pub fn outer_combine_with(
        &mut self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<()> {
        *self = outer_combine(DynT, self, store, that, that_store, f)?;
        Ok(())
    }

    pub fn left_combine(
        &self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<Self> {
        left_combine(DynT, self, store, that, that_store, f)
    }

    pub fn left_combine_with(
        &mut self,
        store: &DynBlobStore,
        that: &Self,
        that_store: &DynBlobStore,
        f: impl Fn(TreeValue, TreeValue) -> anyhow::Result<TreeValue> + Copy,
    ) -> anyhow::Result<()> {
        *self = outer_combine(DynT, self, store, that, that_store, f)?;
        Ok(())
    }
}

impl<'a> FromIterator<(&'a [u8], &'a [u8])> for TreeNode {
    fn from_iter<T: IntoIterator<Item = (&'a [u8], &'a [u8])>>(iter: T) -> Self {
        let store = &NoStore;
        iter.into_iter().fold(TreeNode::empty(), |a, (key, value)| {
            let b = TreeNode::single(key, value);
            outer_combine(NoStoreT, &a, &store, &b, &store, |_, b| Ok(b)).unwrap()
        })
    }
}

impl FromIterator<(Vec<u8>, Vec<u8>)> for TreeNode {
    fn from_iter<T: IntoIterator<Item = (Vec<u8>, Vec<u8>)>>(iter: T) -> Self {
        let store = &NoStore;
        iter.into_iter().fold(TreeNode::empty(), |a, (key, value)| {
            let b = TreeNode::single(&key, &value);
            outer_combine(NoStoreT, &a, &store, &b, &store, |_, b| Ok(b)).unwrap()
        })
    }
}

struct Tree<S: BlobStore> {
    tree: TreeNode,
    store: S,
}

impl<S: BlobStore> Tree<S> {
    fn as_ref(&self) -> TreeRef<'_, S> {
        TreeRef {
            tree: &self.tree,
            store: &self.store,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TreeRef<'a, S: BlobStore> {
    tree: &'a TreeNode,
    store: &'a S,
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
    NotFound {
        // the closest match
        closest: T,
        // number of remaining elements in the prefix of the tree
        rt: usize,
        // number of remaining elements in the search prefix
        rp: usize,
    },
}

/// find a prefix in a tree. Will either return
/// - Found(tree) if we found the tree exactly,
/// - Prefix if we found a tree of which prefix is a prefix
/// - NotFound if there is no tree
fn find<S: BlobStore>(
    store: &S,
    tree: &TreeNode,
    prefix: &[u8],
) -> std::result::Result<FindResult<TreeNode>, S::Error> {
    let tree_prefix = tree.prefix.load(store)?;
    let n = common_prefix(tree_prefix.as_ref(), prefix);
    // remaining in prefix
    let rp = prefix.len() - n;
    // remaining in tree prefix
    let rt = tree_prefix.len() - n;
    Ok(if rp == 0 && rt == 0 {
        // direct hit
        FindResult::Found(tree.clone())
    } else if rp == 0 {
        // tree is a subtree of prefix
        FindResult::Prefix {
            tree: tree.clone(),
            rt,
        }
    } else if rt == 0 {
        // prefix is a subtree of tree
        let c = &prefix[n];
        let tree_children = tree.children.load(store)?;
        let index = if tree_children.len() == 256 {
            // shortcut for full
            Ok(*c as usize)
        } else {
            tree_children.binary_search_by(|e| e.prefix.first_opt().unwrap().cmp(c))
        };
        if let Ok(index) = index {
            let child = &tree_children[index];
            find(store, child, &prefix[n..])?
        } else {
            FindResult::NotFound {
                closest: tree.clone(),
                rp,
                rt,
            }
        }
    } else {
        // disjoint, but we still need to store how far we matched
        FindResult::NotFound {
            closest: tree.clone(),
            rp,
            rt,
        }
    })
}

/// Outer combine two trees with a function f
fn outer_combine2<T: TT>(
    t: T,
    a: &TreeNode,
    ab: &T::AB,
    b: &TreeNode,
    bb: &T::BB,
    f: impl Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
) -> std::result::Result<TreeNode, T::E> {
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.detached(ab);
    let bv = || b.value.detached(bb);
    let prefix = TreePrefix::from_slice(&ap[..n]);
    let children;
    let value;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if a.value.is_none() {
            if b.value.is_none() {
                // both none - none
                TreeValue::none()
            } else {
                // detach and take b
                bv()?
            }
        } else {
            if b.value.is_none() {
                // detach and take a
                av()?
            } else {
                // call the combine fn
                f(av()?, bv()?)?
            }
        };
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &b.children.load(bb)?,
            &bb,
            &OuterCombineOp { f },
        )?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = av()?;
        let b = b.clone_shortened(bb, n)?;
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &[b],
            &bb,
            &OuterCombineOp { f },
        )?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = bv()?;
        let a = a.clone_shortened(ab, n)?;
        children = VecMergeState::<T>::merge(
            &[a],
            &ab,
            &b.children.load(bb)?,
            &bb,
            &OuterCombineOp { f },
        )?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = TreeValue::none();
        // children is just the shortened children a and b in the right order
        let mut a = a.clone_shortened(ab, n)?;
        let mut b = b.clone_shortened(bb, n)?;
        if ap[n] > bp[n] {
            std::mem::swap(&mut a, &mut b);
        }
        children = vec![a, b];
    }
    let mut res = TreeNode {
        prefix,
        value,
        children: TreeChildren::from_vec(children),
    };
    res.unsplit(ab)?;
    Ok(res)
}

/// Outer combine two trees with a function f
fn outer_combine<T: TT>(
    _t: T,
    a: &TreeNode,
    ab: &T::AB,
    b: &TreeNode,
    bb: &T::BB,
    f: impl Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
) -> std::result::Result<TreeNode, T::E> {
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.detached(ab);
    let bv = || b.value.detached(bb);
    let prefix = TreePrefix::from_slice(&ap[..n]);
    let children;
    let value;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if a.value.is_none() {
            if b.value.is_none() {
                // both none - none
                TreeValue::none()
            } else {
                // detach and take b
                bv()?
            }
        } else {
            if b.value.is_none() {
                // detach and take a
                av()?
            } else {
                // call the combine fn
                f(av()?, bv()?)?
            }
        };
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &b.children.load(bb)?,
            &bb,
            &OuterCombineOp { f },
        )?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = av()?;
        let b = b.clone_shortened(bb, n)?;
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &[b],
            &bb,
            &OuterCombineOp { f },
        )?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = bv()?;
        let a = a.clone_shortened(ab, n)?;
        children = VecMergeState::<T>::merge(
            &[a],
            &ab,
            &b.children.load(bb)?,
            &bb,
            &OuterCombineOp { f },
        )?;
    } else {
        // the two nodes are disjoint
        // value is none
        value = TreeValue::none();
        // children is just the shortened children a and b in the right order
        let mut a = a.clone_shortened(ab, n)?;
        let mut b = b.clone_shortened(bb, n)?;
        if ap[n] > bp[n] {
            std::mem::swap(&mut a, &mut b);
        }
        children = vec![a, b];
    }
    let mut res = TreeNode {
        prefix,
        value,
        children: TreeChildren::from_vec(children),
    };
    res.unsplit(ab)?;
    Ok(res)
}

/// Inner combine two trees with a function f
fn inner_combine<T: TT>(
    t: T,
    a: &TreeNode,
    ab: &T::AB,
    b: &TreeNode,
    bb: &T::BB,
    f: impl Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
) -> std::result::Result<TreeNode, T::E> {
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.detached(ab);
    let bv = || b.value.detached(bb);
    let prefix;
    let children;
    let value;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = if !a.value.is_none() && !b.value.is_none() {
            // call the combine fn
            f(av()?, bv()?)?
        } else {
            // any none - none
            TreeValue::none()
        };
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &b.children.load(bb)?,
            &bb,
            &InnerCombineOp { f },
        )?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = TreeValue::none();
        let b = b.clone_shortened(bb, n)?;
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &[b],
            &bb,
            &InnerCombineOp { f },
        )?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = TreeValue::none();
        let a = a.clone_shortened(ab, n)?;
        children = VecMergeState::<T>::merge(
            &[a],
            &ab,
            &b.children.load(bb)?,
            &bb,
            &InnerCombineOp { f },
        )?;
    } else {
        // the two nodes are disjoint
        // value is none
        prefix = TreePrefix::empty();
        children = Vec::new();
        value = TreeValue::none();
    }
    let mut res = TreeNode {
        prefix,
        value,
        children: TreeChildren::from_vec(children),
    };
    res.unsplit(ab)?;
    Ok(res)
}

/// Left combine two trees with a function f
pub fn left_combine<T: TT>(
    t: T,
    a: &TreeNode,
    ab: &T::AB,
    b: &TreeNode,
    bb: &T::BB,
    f: impl Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
) -> std::result::Result<TreeNode, T::E> {
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.detached(ab);
    let bv = || b.value.detached(bb);
    let prefix;
    let children;
    let value;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = if !a.value.is_none() {
            // call the combine fn
            f(av()?, bv()?)?
        } else {
            // any none - none
            TreeValue::none()
        };
        children = VecMergeState::<T>::merge(
            &a.children.load(ab)?,
            &ab,
            &b.children.load(bb)?,
            &bb,
            &LeftCombineOp { f },
        )?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = TreeValue::none();
        let b = b.clone_shortened(bb, n)?;
        children =
            VecMergeState::<T>::merge(&a.children.load(ab)?, &ab, &[b], &bb, &LeftCombineOp { f })?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        prefix = TreePrefix::from_slice(&ap[..n]);
        value = TreeValue::none();
        let a = a.clone_shortened(ab, n)?;
        children =
            VecMergeState::<T>::merge(&[a], &ab, &b.children.load(bb)?, &bb, &LeftCombineOp { f })?;
    } else {
        // the two nodes are disjoint
        // just take a as it is, but detach it
        return Ok(a.detached(ab)?);
    }
    let mut res = TreeNode {
        prefix,
        value,
        children: TreeChildren::from_vec(children),
    };
    res.unsplit(ab)?;
    Ok(res)
}

/// Left combine two trees with a function f
fn intersects(
    a: &TreeNode,
    ab: &DynBlobStore,
    b: &TreeNode,
    bb: &DynBlobStore,
) -> anyhow::Result<bool> {
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.detached(ab);
    let bv = || b.value.detached(bb);
    let res;
    if n == ap.len() && n == bp.len() {
        todo!()
        // let r = (a.value.is_some() && b.value.is_some()) ||
        //     BoolOpMergeState::merge(
        //         &a.children.load_prefixes(ab)?,
        //         &b.children.load_prefixes(bb)?,
        //         IntersectOp { ab, bb });
    } else if n == ap.len() {
        todo!()
    } else if n == bp.len() {
        todo!()
    } else {
        res = false
    }
    Ok(res)
}

#[derive(Debug, Clone)]
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

impl core::ops::Deref for IterKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// An iterator over the values of a radix tree.
///
/// This is more efficient than taking the value part of an entry iteration, because the keys
/// do not have to be constructed.
pub struct Values<'a> {
    stack: Vec<(TreeNode, usize)>,
    store: &'a DynBlobStore,
}

impl<'a> Values<'a> {
    fn new(tree: TreeNode, store: &'a DynBlobStore) -> Self {
        Self {
            stack: vec![(tree, 0)],
            store,
        }
    }

    fn tree(&self) -> &TreeNode {
        &self.stack.last().unwrap().0
    }

    fn inc(&mut self) -> Option<usize> {
        let pos = &mut self.stack.last_mut().unwrap().1;
        let res = if *pos == 0 { None } else { Some(*pos - 1) };
        *pos += 1;
        res
    }

    fn next0(&mut self) -> anyhow::Result<Option<TreeValue>> {
        while !self.stack.is_empty() {
            if let Some(pos) = self.inc() {
                let children = self.tree().children.load(self.store)?;
                if pos < children.len() {
                    self.stack.push((children[pos].clone(), 0));
                } else {
                    self.stack.pop();
                }
            } else if self.tree().value.is_some() {
                return Ok(Some(self.tree().value.clone()));
            }
        }
        Ok(None)
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = anyhow::Result<TreeValue>;

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

pub struct GroupBy<'a, F> {
    path: IterKey,
    stack: Vec<(TreeNode, usize)>,
    store: &'a DynBlobStore,
    descend: F,
}

impl<'a, F: Fn(&[u8], &TreeNode) -> bool> GroupBy<'a, F> {
    fn new(tree: &'a TreeNode, store: &'a DynBlobStore, prefix: IterKey, descend: F) -> Self {
        Self {
            stack: vec![(tree.clone(), 0)],
            path: prefix,
            store,
            descend,
        }
    }

    fn tree(&self) -> &TreeNode {
        &self.stack.last().unwrap().0
    }

    fn inc(&mut self) -> Option<usize> {
        let pos = &mut self.stack.last_mut().unwrap().1;
        let res = if *pos == 0 { None } else { Some(*pos - 1) };
        *pos += 1;
        res
    }

    fn prefix(&self) -> TreePrefix {
        TreePrefix::from_arc_vec(self.path.0.clone())
    }

    fn push(&mut self, child: TreeNode) -> anyhow::Result<()> {
        let child_prefix = child.prefix.load(self.store)?;
        self.path.append(child_prefix.as_ref());
        self.stack.push((child, 0));
        Ok(())
    }

    fn pop(&mut self) -> anyhow::Result<()> {
        let prefix = self.tree().prefix.load(self.store)?;
        self.path.pop(prefix.len());
        self.stack.pop();
        Ok(())
    }

    fn next0(&mut self) -> anyhow::Result<Option<TreeNode>> {
        while !self.stack.is_empty() {
            if let Some(pos) = self.inc() {
                let children = self.tree().children.load(self.store)?;
                if pos < children.len() {
                    self.push(children[pos].clone())?;
                } else {
                    self.pop()?;
                }
            } else {
                let tree = self.tree();
                if (self.descend)(&self.path, tree) {
                    if tree.value.is_some() {
                        let res = TreeNode {
                            prefix: self.prefix(),
                            value: tree.value.clone(),
                            children: TreeChildren::empty(),
                        };
                        return Ok(Some(res));
                    }
                } else {
                    if !tree.is_empty() {
                        let res = TreeNode {
                            prefix: self.prefix(),
                            value: tree.value.clone(),
                            children: tree.children.clone(),
                        };
                        self.pop()?;
                        return Ok(Some(res));
                    } else {
                        self.pop()?;
                    }
                }
            }
        }
        Ok(None)
    }
}

impl<'a, F: Fn(&[u8], &TreeNode) -> bool> Iterator for GroupBy<'a, F> {
    type Item = anyhow::Result<TreeNode>;

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

pub struct Iter<'a, S> {
    path: IterKey,
    stack: Vec<(TreeNode, usize)>,
    store: &'a S,
}

impl<'a, S: BlobStore> Iter<'a, S> {
    fn empty(store: &'a S) -> Self {
        Self {
            stack: Vec::new(),
            path: IterKey::new(&[]),
            store,
        }
    }

    fn new(tree: TreeNode, store: &'a S, prefix: IterKey) -> Self {
        Self {
            stack: vec![(tree, 0)],
            path: prefix,
            store,
        }
    }

    fn tree(&self) -> &TreeNode {
        &self.stack.last().unwrap().0
    }

    fn inc(&mut self) -> Option<usize> {
        let pos = &mut self.stack.last_mut().unwrap().1;
        let res = if *pos == 0 { None } else { Some(*pos - 1) };
        *pos += 1;
        res
    }

    fn next0(&mut self) -> std::result::Result<Option<(IterKey, TreeValue)>, S::Error> {
        while !self.stack.is_empty() {
            if let Some(pos) = self.inc() {
                let children = self.tree().children.load(self.store)?;
                if pos < children.len() {
                    let child = children[pos].clone();
                    let child_prefix = child.prefix.load(self.store)?;
                    self.path.append(child_prefix.as_ref());
                    self.stack.push((child, 0));
                } else {
                    let prefix = self.tree().prefix.load(self.store)?;
                    self.path.pop(prefix.len());
                    self.stack.pop();
                }
            } else if self.tree().value.is_some() {
                return Ok(Some((self.path.clone(), self.tree().value.clone())));
            }
        }
        Ok(None)
    }
}

impl<'a, S: BlobStore> Iterator for Iter<'a, S> {
    type Item = std::result::Result<(IterKey, TreeValue), S::Error>;

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

struct OuterCombineOp<F> {
    f: F,
}

impl<'a, T, F> MergeOperation<VecMergeState<'a, T>> for OuterCombineOp<F>
where
    F: Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
    T: TT,
{
    fn cmp(&self, a: &TreeNode, b: &TreeNode) -> Ordering {
        a.prefix.first_opt().cmp(&b.prefix.first_opt())
    }
    fn from_a(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_a(n, true)
    }
    fn from_b(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_b(n, true)
    }
    fn collision(&self, m: &mut VecMergeState<'a, T>) -> bool {
        let a = m.a.next().unwrap();
        let b = m.b.next().unwrap();
        match outer_combine2(T::default(), a, m.ab, b, m.bb, self.f) {
            Ok(res) => {
                m.r.push(res);
                true
            }
            Err(cause) => {
                m.err = Some(cause);
                false
            }
        }
    }
}

struct InnerCombineOp<F> {
    f: F,
}

impl<'a, T, F> MergeOperation<VecMergeState<'a, T>> for InnerCombineOp<F>
where
    T: TT,
    F: Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
{
    fn cmp(&self, a: &TreeNode, b: &TreeNode) -> Ordering {
        a.prefix.first_opt().cmp(&b.prefix.first_opt())
    }
    fn from_a(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_a(n, false)
    }
    fn from_b(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_b(n, false)
    }
    fn collision(&self, m: &mut VecMergeState<'a, T>) -> bool {
        let a = m.a.next().unwrap();
        let b = m.b.next().unwrap();
        match inner_combine(T::default(), a, m.ab, b, m.bb, self.f) {
            Ok(res) => {
                m.r.push(res);
                true
            }
            Err(cause) => {
                m.err = Some(cause);
                false
            }
        }
    }
}
struct LeftCombineOp<F> {
    f: F,
}

impl<'a, T, F> MergeOperation<VecMergeState<'a, T>> for LeftCombineOp<F>
where
    T: TT,
    F: Fn(TreeValue, TreeValue) -> std::result::Result<TreeValue, T::E> + Copy,
{
    fn cmp(&self, a: &TreeNode, b: &TreeNode) -> Ordering {
        a.prefix.first_opt().cmp(&b.prefix.first_opt())
    }
    fn from_a(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_a(n, true)
    }
    fn from_b(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_b(n, false)
    }
    fn collision(&self, m: &mut VecMergeState<'a, T>) -> bool {
        let a = m.a.next().unwrap();
        let b = m.b.next().unwrap();
        match left_combine(T::default(), a, m.ab, b, m.bb, self.f) {
            Ok(res) => {
                m.r.push(res);
                true
            }
            Err(cause) => {
                m.err = Some(cause);
                false
            }
        }
    }
}

struct IntersectOp {}

impl<'a, T: TT> MergeOperation<VecMergeState<'a, T>> for IntersectOp {
    fn cmp(&self, a: &TreeNode, b: &TreeNode) -> Ordering {
        a.prefix.first_opt().cmp(&b.prefix.first_opt())
    }
    fn from_a(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_a(n, false)
    }
    fn from_b(&self, m: &mut VecMergeState<'a, T>, n: usize) -> bool {
        m.advance_b(n, false)
    }
    fn collision(&self, m: &mut VecMergeState<'a, T>) -> bool {
        let a = &m.a_slice()[0];
        let b = &m.b_slice()[0];
        // if this is true, we have found an intersection and can abort.
        todo!()
        // let take = intersects(a, self.ab, b, self.bb);
        // m.advance_a(1, take) && m.advance_b(1, false)
    }
}

#[cfg(test)]
mod tests {
    use crate::blob_store::no_store_dyn;

    use super::*;
    use proptest::prelude::*;
    use std::{collections::BTreeMap, convert::Infallible, mem::size_of, sync::Arc};

    #[derive(Debug, PartialEq, Eq)]
    enum Jsonish {
        String(String),
        Map(BTreeMap<String, Jsonish>),
    }

    impl Jsonish {
        /// flatten to a radix tree, using a separator char '.'
        fn to_radix_tree_separated(&self) -> TreeNode {
            match self {
                Jsonish::String(v) => {
                    assert!(!v.is_empty());
                    TreeNode::leaf(v.as_bytes())
                }
                Jsonish::Map(map) => map
                    .iter()
                    .map(|(k, v)| {
                        assert!(!k.is_empty());
                        match v {
                            Jsonish::String(v) => {
                                assert!(!v.is_empty());
                                TreeNode::single(k.as_bytes(), v.as_bytes())
                            }
                            Jsonish::Map(_) => {
                                let mut res = TreeNode::new(
                                    format!("{}.", k).as_bytes(),
                                    None,
                                    &[v.to_radix_tree_separated()],
                                );
                                res.unsplit(no_store_dyn()).unwrap();
                                res
                            }
                        }
                    })
                    .fold(TreeNode::empty(), |a, b| {
                        outer_combine(DynT, &a, no_store_dyn(), &b, no_store_dyn(), |_, b| Ok(b))
                            .unwrap()
                    }),
            }
        }

        /// unfltten from a radix tree, using a separator char '.'
        fn from_radix_tree_separated(tree: &TreeNode) -> anyhow::Result<Self> {
            let sep = ".".as_bytes()[0];
            Ok(if tree.prefix.is_empty() && tree.value.is_some() {
                Jsonish::String(
                    std::str::from_utf8(
                        tree.value.load(no_store_dyn()).unwrap().unwrap().as_ref(),
                    )?
                    .to_owned(),
                )
            } else {
                let iter = tree.try_group_by(no_store_dyn(), |p, _| !p.contains(&sep))?;
                Jsonish::Map(
                    iter.map(|child| {
                        let mut child = child?;
                        let prefix = child.prefix.load(no_store_dyn())?;
                        Ok(if let Some(p) = prefix.iter().position(|x| *x == sep) {
                            let key = std::str::from_utf8(&prefix[..p])?.to_owned();
                            child.prefix = TreePrefix::from_slice(&prefix[p + 1..]);
                            (key, Self::from_radix_tree_separated(&child)?)
                        } else {
                            let prefix = std::str::from_utf8(prefix.as_ref())?.to_owned();
                            (
                                prefix,
                                Jsonish::String(
                                    std::str::from_utf8(
                                        child.value.load(no_store_dyn()).unwrap().unwrap().as_ref(),
                                    )?
                                    .to_owned(),
                                ),
                            )
                        })
                    })
                    .collect::<anyhow::Result<BTreeMap<_, _>>>()?,
                )
            })
        }
    }

    fn arb_jsonish_tree_no_empty() -> impl Strategy<Value = Jsonish> {
        let leaf = prop_oneof!["[a-z]+".prop_map(Jsonish::String),];

        leaf.prop_recursive(
            4,  // No more than 4 branch levels deep
            64, // Target around 64 total elements
            8,  // Each collection is up to 16 elements long
            |element| {
                prop_oneof![
                    prop::collection::btree_map("[a-z]+", element, 1..16).prop_map(Jsonish::Map)
                ]
            },
        )
    }

    fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(b'0'..b'9', 0..9)
    }

    fn arb_value() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..9)
    }

    fn arb_tree_contents() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
        proptest::collection::btree_map(arb_prefix(), arb_value(), 0..10)
    }

    fn arb_owned_tree() -> impl Strategy<Value = TreeNode> {
        arb_tree_contents().prop_map(|x| mk_owned_tree(&x))
    }

    fn mk_owned_tree(v: &BTreeMap<Vec<u8>, Vec<u8>>) -> TreeNode {
        v.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect()
    }

    fn to_btree_map(t: &TreeNode) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let store = no_store_dyn();
        t.try_iter(store)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                let data = v.load(store).unwrap().unwrap();
                (k.to_vec(), data.to_vec())
            })
            .collect()
    }

    impl Arbitrary for TreeNode {
        type Parameters = ();
        type Strategy = BoxedStrategy<TreeNode>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_owned_tree().boxed()
        }
    }

    proptest! {
        #[test]
        fn tree_dump(x in arb_owned_tree()) {
            x.dump_tree(no_store_dyn()).unwrap();
            println!();
        }

        #[test]
        fn tree_iter(x in arb_owned_tree()) {
            let iter = x.try_iter(no_store_dyn()).unwrap();
            println!();
            x.dump_tree(no_store_dyn()).unwrap();
            println!();
            for r in iter {
                let (k, v) = r.unwrap();
                let data = v.load(no_store_dyn()).unwrap().unwrap();
                println!("{}: {}", Hex::new(&k), Hex::new(data.as_ref()));
            }
            println!();
        }

        #[test]
        fn btreemap_tree_roundtrip(x in arb_tree_contents()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let actual = to_btree_map(&tree);
            prop_assert_eq!(reference, actual);
        }

        #[test]
        fn attach_detach_roundtrip(x in arb_tree_contents()) {
            let reference = x;
            let mut tree = mk_owned_tree(&reference);
            let mut store: DynBlobStore = Box::new(MemStore::default());
            tree.attach(&mut store).unwrap();
            tree.detach(&store).unwrap();
            let actual = to_btree_map(&tree);
            prop_assert_eq!(reference, actual);
        }

        #[test]
        fn btreemap_tree_values_roundtrip(x in arb_tree_contents()) {
            let reference = x.values().cloned().collect::<Vec<_>>();
            let tree = mk_owned_tree(&x);
            let actual = tree.try_values(no_store_dyn()).map(|r| {
                r.unwrap().load(no_store_dyn()).unwrap().unwrap().to_vec()
            }).collect::<Vec<_>>();
            prop_assert_eq!(reference, actual);
        }

        #[test]
        fn get_contains(x in arb_tree_contents()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            for (k, v) in reference {
                prop_assert!(tree.contains_key(no_store_dyn(), &k).unwrap());
                prop_assert_eq!(tree.get(no_store_dyn(), &k).unwrap(), Some(Blob::from_slice(&v)));
            }
        }

        #[test]
        fn scan_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let filtered = tree.scan_prefix(no_store_dyn(), &prefix).unwrap();
            for r in filtered {
                let (k, v) = r.unwrap();
                prop_assert!(k.as_ref().starts_with(&prefix));
                let v = v.load(no_store_dyn()).unwrap().unwrap();
                let t = reference.get(k.as_ref()).unwrap();
                prop_assert_eq!(v.as_ref(), t);
            }
        }

        #[test]
        fn filter_prefix(x in arb_tree_contents(), prefix in any::<Vec<u8>>()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let filtered = tree.filter_prefix(no_store_dyn(), &prefix).unwrap();
            for r in filtered.try_iter(no_store_dyn()).unwrap() {
                let (k, v) = r.unwrap();
                prop_assert!(k.as_ref().starts_with(&prefix));
                let v = v.load(no_store_dyn()).unwrap().unwrap();
                let t = reference.get(k.as_ref()).unwrap();
                prop_assert_eq!(v.as_ref(), t);
            }
        }

        #[test]
        fn biased_union(a in arb_tree_contents(), b in arb_tree_contents()) {
            let at = mk_owned_tree(&a);
            let bt = mk_owned_tree(&b);
            // check right biased union
            let rbut = outer_combine(DynT, &at, no_store_dyn(), &bt, no_store_dyn(), |_, b| Ok(b)).unwrap();
            let rbu = to_btree_map(&rbut);
            let mut rbu_reference = a.clone();
            for (k, v) in b.clone() {
                rbu_reference.insert(k, v);
            }
            prop_assert_eq!(rbu, rbu_reference);
            // check left biased union
            let lbut = outer_combine(DynT, &at, no_store_dyn(), &bt, no_store_dyn(), |a, _| Ok(a)).unwrap();
            let lbu = to_btree_map(&lbut);
            let mut lbu_reference = b.clone();
            for (k, v) in a.clone() {
                lbu_reference.insert(k, v);
            }
            prop_assert_eq!(lbu, lbu_reference);
        }

        #[test]
        fn biased_intersection(a in arb_tree_contents(), b in arb_tree_contents()) {
            let at = mk_owned_tree(&a);
            let bt = mk_owned_tree(&b);
            // check right biased intersection
            let rbut = inner_combine(DynT, &at, no_store_dyn(), &bt, no_store_dyn(), |_, b| Ok(b)).unwrap();
            let rbu = to_btree_map(&rbut);
            let mut rbu_reference = b.clone();
            rbu_reference.retain(|k, _| a.contains_key(k));
            prop_assert_eq!(rbu, rbu_reference);
            // check left biased intersection
            let lbut = inner_combine(DynT, &at, no_store_dyn(), &bt, no_store_dyn(), |a, _| Ok(a)).unwrap();
            let lbu = to_btree_map(&lbut);
            let mut lbu_reference = a.clone();
            lbu_reference.retain(|k, _| b.contains_key(k));
            prop_assert_eq!(lbu, lbu_reference);
        }


        #[test]
        fn group_by_true(a in arb_tree_contents()) {
            let at = mk_owned_tree(&a);
            let trees: Vec<TreeNode> = at.try_group_by(no_store_dyn(), |_, _| true).unwrap().collect::<anyhow::Result<Vec<_>>>().unwrap();
            prop_assert_eq!(a.len(), trees.len());
            for ((k0, v0), tree) in a.iter().zip(trees) {
                let k0: &[u8] = &k0;
                let v0: &[u8] = &v0;
                let k1 = tree.prefix.load(no_store_dyn()).unwrap().to_vec();
                let v1 = tree.value.load(no_store_dyn()).unwrap().map(|x| x.to_vec());
                prop_assert!(tree.children.is_empty());
                prop_assert_eq!(k0, k1);
                prop_assert_eq!(Some(v0.to_vec()), v1);
            }
        }

        #[test]
        fn group_by_tree_separated(a in arb_jsonish_tree_no_empty()) {
            let tree = a.to_radix_tree_separated();
            let b = Jsonish::from_radix_tree_separated(&tree).unwrap();
            prop_assert_eq!(a, b);
        }

        #[test]
        fn group_by_fixed(a in arb_tree_contents(), n in 0usize..8) {
            let at = mk_owned_tree(&a);
            let trees: Vec<TreeNode> = at.try_group_by(no_store_dyn(), |x, _| x.len() <= n).unwrap().collect::<anyhow::Result<Vec<_>>>().unwrap();
            prop_assert!(trees.len() <= a.len());
            let mut leafs = BTreeMap::new();
            for tree in &trees {
                let prefix = tree.prefix.load(no_store_dyn()).unwrap();
                if prefix.len() <= n {
                    prop_assert!(tree.children.is_empty());
                    prop_assert!(tree.value.is_some());
                    let value = tree.value.load(no_store_dyn()).unwrap().unwrap();
                    let prev = leafs.insert(prefix.to_vec(), value.to_vec());
                    prop_assert!(prev.is_none());
                } else {
                    for x in tree.try_iter(no_store_dyn()).unwrap() {
                        let (k, v) = x.unwrap();
                        let prev = leafs.insert(k.to_vec(), v.load(no_store_dyn()).unwrap().unwrap().to_vec());
                        prop_assert!(prev.is_none());
                    }
                }
            }
            prop_assert_eq!(a, leafs);
        }
    }

    #[test]
    fn from_iter() {
        let elems: Vec<(&[u8], &[u8])> = vec![(b"a", b"b")];
        let tree: TreeNode = elems.into_iter().collect();
        tree.dump_tree(&NoStoreDyn::new()).unwrap();
    }

    #[test]
    fn flex_ref_debug() {
        let x = FlexRef::<()>::none();
        println!("{:?}", x);
        let x = FlexRef::<()>::inline_from_slice(b"abcd");
        println!("{:?}", x);
        let x = FlexRef::<()>::id_from_u64(1234);
        println!("{:?}", x);
        let arc = Arc::new(b"abcd".to_vec());
        let a1 = arc.clone();
        let a2 = arc.clone();
        // base addr of the arc
        let u1: usize = unsafe { std::mem::transmute(a1) };
        // content addr of the arc, 16 bytes larger on 64 bit
        let u2 = Arc::into_raw(a2) as usize;
        // println!("{} {} {}", u1, u2, u2 - u1);
        let x = FlexRef::owned_from_arc(arc);
        println!("{:?}", x);
    }

    #[test]
    fn tree_node_create() -> anyhow::Result<()> {
        let store = no_store_dyn();
        let node = TreeNode::leaf(b"abcd");
        assert!(node.prefix.load(store)? == Blob::<u8>::from_slice(&[]));
        assert!(node.value.load(store)? == Some(Blob::<u8>::from_slice(b"abcd")));
        assert!(node.children.load(store)?.is_empty());
        println!("{:?}", node);
        let node = TreeNode::leaf(b"abcdefgh");
        println!("{:?}", node);
        let node = TreeNode::single(b"abcd", b"ijklmnop");
        println!("{:?}", node);
        let node = TreeNode::single(b"abcdefgh", b"ijklmnop");
        println!("{:?}", node);
        let node = TreeNode::new(b"a", None, &[node]);
        println!("{:?}", node);
        node.dump_tree(&store)?;
        Ok(())
    }

    #[test]
    fn tree_node_attach_detach() -> anyhow::Result<()> {
        let mut store: DynBlobStore = Box::new(MemStore::default());
        let node = TreeNode::single(b"abcdefgh", b"ijklmnop");
        let mut node = TreeNode::new(b"a", None, &[node]);
        println!("{:?}", node);
        node.attach(&mut store)?;
        println!("{:?}", node);
        node.detach(&mut store)?;
        println!("{:?}", node);
        println!("{:?}", store);
        Ok(())
    }

    #[test]
    fn union_large() -> anyhow::Result<()> {
        let mut nodes = (0..1000u64).map(|i| {
            let key = i.to_string() + "000000000";
            let value = i.to_string() + "000000000";
            TreeNode::single(key.as_bytes(), value.as_bytes())
        });
        let mut store: DynBlobStore = Box::new(MemStore::default());
        let mut res = nodes.try_fold(TreeNode::empty(), |a, b| {
            outer_combine(DynT, &a, &store, &b, &store, |_, b| Ok(b))
        })?;
        res.dump_tree(&store)?;
        res.attach(&mut store)?;
        println!("{:?}", res);
        res.dump_tree(&store)?;
        Ok(())
    }

    #[test]
    fn union() -> anyhow::Result<()> {
        let store = no_store_dyn();

        println!("disjoint");
        let a = TreeNode::single(b"a", b"1");
        let b = TreeNode::single(b"b", b"2");
        let t = outer_combine(DynT, &a, &store, &b, &store, |_, b| Ok(b))?;
        t.dump_tree(&store)?;

        println!("a prefix of b");
        let a = TreeNode::single(b"a", b"1");
        let b = TreeNode::single(b"ab", b"2");
        let t = outer_combine(DynT, &a, &store, &b, &store, |_, b| Ok(b))?;
        t.dump_tree(&store)?;

        println!("b prefix of a");
        let a = TreeNode::single(b"ab", b"1");
        let b = TreeNode::single(b"a", b"2");
        let t = outer_combine(DynT, &a, &store, &b, &store, |_, b| Ok(b))?;
        t.dump_tree(&store)?;

        println!("same prefix");
        let a = TreeNode::single(b"ab", b"1");
        let b = TreeNode::single(b"ab", b"2");
        let t = outer_combine(DynT, &a, &store, &b, &store, |_, b| Ok(b))?;
        t.dump_tree(&store)?;

        Ok(())
    }

    #[test]
    fn sizes() {
        enum Nope {}
        assert_eq!(size_of::<Infallible>(), 0);
        assert_eq!(size_of::<std::result::Result<u64, Infallible>>(), 8);
        assert_eq!(size_of::<Option<Infallible>>(), 0);
        assert_eq!(size_of::<Nope>(), 0);
        assert_eq!(size_of::<std::result::Result<u64, Nope>>(), 8);
        assert_eq!(size_of::<Option<Nope>>(), 0);
    }
}
