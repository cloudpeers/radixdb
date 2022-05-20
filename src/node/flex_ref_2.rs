use std::{cmp::Ordering, marker::PhantomData, ops::Deref, sync::Arc};

use crate::{
    store::{unwrap_safe, BlobStore, NoError, NoStore, Blob},
    Hex,
};
use std::fmt::Debug;

use super::OwnedSlice;

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
struct TreePrefix<S = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreePrefix<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreePrefix({:?})", &self.1)
    }
}

impl<S: BlobStore> TreePrefix<S> {

    fn first_opt(&self) -> Option<u8> {
        self.1.first_u8_opt()
    }

    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn load(&self, store: &S) -> Result<OwnedSlice<u8>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            OwnedSlice::from_slice(x)
        } else if let Some(x) = self.1.arc_as_clone() {
            OwnedSlice::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_u64() {
            OwnedSlice::from_blob(store.read(id)?)
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

    fn drop(&self) {
        self.1.with_arc(|arc| unsafe {
            Arc::decrement_strong_count(Arc::as_ptr(arc));
        });
    }
}

impl AsRef<[u8]> for TreePrefix {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}

#[repr(transparent)]
struct TreeValue<S: BlobStore = NoStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreeValue<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeValue({:?})", &self.1)
    }
}

impl<S: BlobStore> TreeValue<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
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

    fn load(&self, store: &S) -> Result<Option<OwnedSlice<u8>>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            Some(OwnedSlice::from_slice(x))
        } else if let Some(x) = self.1.arc_as_clone() {
            Some(OwnedSlice::from_arc_vec(x))
        } else if let Some(id) = self.1.id_as_u64() {
            Some(OwnedSlice::from_blob(store.read(id)?))
        } else {
            None
        })
    }

    fn read_one<'a>(value: &'a [u8]) -> Result<(&'a Self, &'a [u8]), S::Error> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Ok((Self::new(f), rest))
    }

    fn drop(&self) {
        self.1.with_arc(|arc| unsafe {
            Arc::decrement_strong_count(Arc::as_ptr(arc));
        });
    }
}

#[repr(transparent)]
struct TreeChildren<S: BlobStore>(PhantomData<S>, FlexRef<Vec<u8>>);

impl<S: BlobStore> Debug for TreeChildren<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.1.tpe() {
            Type::Inline => write!(f, "TreeChildren::Empty"),
            Type::Arc => write!(f, "TreeChildren::Arc({:?})", self.1.arc_as_clone().unwrap()),
            Type::Id => write!(f, "TreeChildren::Id({:?})", self.1.id_as_u64().unwrap()),
            Type::None => write!(f, "TreeChildren invalid"),
        }
    }
}

impl<S: BlobStore> TreeChildren<S> {
    fn new(value: &FlexRef<Vec<u8>>) -> &Self {
        // todo: use ref_cast
        unsafe { std::mem::transmute(value) }
    }

    fn read_one<'a>(value: &'a [u8]) -> Result<(&'a Self, &'a [u8]), S::Error> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read_one(value)?;
        Ok((Self::new(f), rest))
    }

    fn load(&self, store: &S) -> Result<CIW<S>, S::Error> {
        Ok(if let Some(x) = self.1.inline_as_ref() {
            assert!(x.is_empty());
            CIW::empty()
        } else if let Some(x) = self.1.arc_as_clone() {
            CIW::from_arc_vec(x)
        } else if let Some(id) = self.1.id_as_u64() {
            CIW::from_blob(store.read(id)?)
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

    fn drop(&self) {
        self.1.with_arc(|arc| unsafe {
            Arc::decrement_strong_count(Arc::as_ptr(arc));
        });
    }
}

struct CIW<S: BlobStore>(OwnedSlice<u8>, PhantomData<S>);

impl<S: BlobStore> CIW<S> {
    fn empty() -> Self {
        Self(OwnedSlice::empty(), PhantomData)
    }

    fn from_arc_vec(value: Arc<Vec<u8>>) -> Self {
        Self(OwnedSlice::from_arc_vec(value), PhantomData)
    }

    fn from_blob(value: Blob) -> Self {
        Self(OwnedSlice::from_blob(value), PhantomData)
    }

    fn iter(&self) -> TreeChildrenIterator<'_, S> {
        TreeChildrenIterator(self.0.as_ref(), PhantomData)
    }
}

#[repr(transparent)]
struct TreeChildrenIterator<'a, S>(&'a [u8], PhantomData<S>);

impl<'a, S: BlobStore> TreeChildrenIterator<'a, S> {
    fn peek(&self) -> Option<Result<Option<u8>, S::Error>> {
        if self.0.is_empty() {
            None
        } else {
            Some(TreePrefix::<S>::read(&self.0).map(|x| x.first_opt()))
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

struct PairIterator<'a, S: BlobStore>(TreeChildrenIterator<'a, S>);

impl<'a, S: BlobStore> Iterator for PairIterator<'a, S> {
    type Item = Result<(Option<u8>, TreeNode<'a, S>), S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|r| r.map(|x| (x.prefix.first_opt(), x)))
    }
}

struct TreeNode<'a, S: BlobStore> {
    prefix: &'a TreePrefix<S>,
    value: &'a TreeValue<S>,
    children: &'a TreeChildren<S>,
}

impl<'a, S: BlobStore> std::fmt::Debug for TreeNode<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeNode")
            .field("prefix", &self.prefix)
            .field("value", &self.value)
            .field("children", &self.children)
            .finish()
    }
}

impl<'a, S: BlobStore + 'static> TreeNode<'a, S> {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.value.is_none()
    }

    fn empty() -> Self {
        Self {
            prefix: TreePrefix::empty(),
            value: TreeValue::none(),
            children: TreeChildren::empty(),
        }
    }

    fn read(buffer: &'a [u8]) -> Result<Self, S::Error> {
        Ok(Self::read_one(buffer)?.0)
    }

    fn read_one(buffer: &'a [u8]) -> Result<(Self, &'a [u8]), S::Error> {
        let (prefix, buffer) = TreePrefix::read_one(buffer)?;
        let (value, buffer) = TreeValue::read_one(buffer)?;
        let (children, buffer) = TreeChildren::read_one(buffer)?;
        Ok((
            Self {
                prefix,
                value,
                children,
            },
            buffer,
        ))
    }

    fn drop(&self) {
        self.prefix.drop();
        self.value.drop();
        self.children.drop();
    }

    fn dump(&self, indent: usize, store: &S) -> Result<(), S::Error> {
        let spacer = std::iter::repeat(" ").take(indent).collect::<String>();
        println!("{}TreeNode", spacer);
        println!("{}  prefix={:?}", spacer, self.prefix);
        println!("{}  value={:?}", spacer, self.value);
        println!("{}  children", spacer);
        for child in self.children.load(store)?.iter() {
            let child = child?;
            child.dump(indent + 4, store)?;
        }
        Ok(())
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
            let l = (data.len() as u8) | 0x80;
            target.push(l);
            target.extend_from_slice(data);
        } else {
            let ptr = Arc::new(data.to_vec());
            target.push(0x01);
            let data: [u8; 8] = unsafe { std::mem::transmute(ptr) };
            target.extend_from_slice(&data);
        }
    }
}

trait FlexRefExt {

    fn push_flexref_none(&mut self);

    fn push_flexref_arc<T>(&mut self, arc: Arc<T>);

    fn push_flexref_inline(&mut self, data: &[u8]);

    fn push_flexref_arc_or_inline(&mut self, data: OwnedSlice<u8>, max_inline: usize);
}

impl FlexRefExt for Vec<u8> {
    fn push_flexref_arc<T>(&mut self, arc: Arc<T>) {
        self.push(0x01);
        let data: [u8; 8] = unsafe { std::mem::transmute(arc) };
        self.extend_from_slice(&data);
    }

    fn push_flexref_inline(&mut self, data: &[u8]) {
        assert!(data.len() < 128);
        let l = (data.len() as u8) | 0x80;
        self.push(l);
        self.extend_from_slice(data);
    }

    fn push_flexref_arc_or_inline(&mut self, data: OwnedSlice<u8>, max_inline: usize) {
        assert!(max_inline < 128);
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

    fn arc(data: Arc<T>, target: &mut Vec<u8>) {

    }

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

    fn len(&self) -> usize {
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

    fn id_as_u64(&self) -> Option<u64> {
        self.with_id(|x| x)
    }

    fn with_arc<U>(&self, f: impl Fn(&Arc<T>) -> U) -> Option<U> {
        if self.tpe() == Type::Arc {
            let mut t = [0u8; 8];
            t.copy_from_slice(&self.2[0..8]);
            let arc: Arc<T> = unsafe { std::mem::transmute(t) };
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

impl FlexRef<Vec<u8>> {
    fn peek<R>(&self, f: impl Fn(&[u8]) -> R) -> R {
        if let Some(inline) = self.inline_as_ref() {
            f(inline)
        } else if let Some(res) = self.with_arc(|x| f(x.as_ref())) {
            res
        } else {
            panic!()
        }
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
struct NodeSeq<S: BlobStore>(PhantomData<S>, [u8]);

impl<S: BlobStore> NodeSeq<S> {
    fn new(value: &[u8]) -> &Self {
        unsafe { std::mem::transmute(value) }
    }

    fn iter(&self) -> TreeChildrenIterator<'_, S> {
        TreeChildrenIterator(&self.1, PhantomData)
    }

    fn entries(&self) -> PairIterator<'_, S> {
        PairIterator(self.iter())
    }
}

struct OwnedNodeSeq<S: BlobStore = NoStore>(Vec<u8>, PhantomData<S>);

impl<S: BlobStore> Debug for OwnedNodeSeq<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<S: BlobStore> OwnedNodeSeq<S> {
    fn new() -> Self {
        Self(Vec::new(), PhantomData)
    }

    fn push_prefix(&mut self, prefix: OwnedSlice<u8>) {
        self.0.push_flexref_arc_or_inline(prefix, 32);
    }

    fn push_value(&mut self, value: Option<OwnedSlice<u8>>) {
        if let Some(value) = value {
            self.0.push_flexref_arc_or_inline(value, 32);
        } else {
            self.0.push_flexref_none()
        }
    }
    
    fn push_children(&mut self, value: OwnedNodeSeq<S>) {
        if !value.0.is_empty() {
            self.0.push_flexref_arc(Arc::new(value))
        } else {
            self.0.push_flexref_none();
        }
    }

    fn empty_tree() -> Self {
        Self::from_node(TreeNode::empty())
    }

    fn from_node(node: TreeNode<'_, S>) -> Self {
        let mut res = Self(Vec::new(), PhantomData);
        node.prefix.1.copy_to(&mut res.0);
        node.value.1.copy_to(&mut res.0);
        node.children.1.copy_to(&mut res.0);
        res
    }

    fn push_new(
        &mut self,
        prefix: OwnedSlice<u8>,
        value: Option<OwnedSlice<u8>>,
        children: OwnedNodeSeq<S>,
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
        let t: &mut OwnedNodeSeq<S2> = unsafe { std::mem::transmute(self) };
        Ok(t.push(node))
    }

    fn push_new_unsplit(
        &mut self,
        prefix: OwnedSlice<u8>,
        value: Option<OwnedSlice<u8>>,
        children: OwnedNodeSeq<S>,
        store: &S,
    ) -> Result<(), S::Error> {
        // todo
        Ok(self.push_new(prefix, value, children))
    }

    fn push(&mut self, node: TreeNode<'_, S>) {
        node.prefix.1.copy_to(&mut self.0);
        node.value.1.copy_to(&mut self.0);
        node.children.1.copy_to(&mut self.0);
    }

    fn push_shortened(
        &mut self,
        node: &TreeNode<'_, S>,
        store: &S,
        n: usize,
    ) -> Result<(), S::Error> {
        if n > 0 {
            let prefix = node.prefix.load(store)?;
            assert!(n < prefix.len());
            FlexRef::slice(&prefix[n..], &mut self.0);
        } else {
            node.prefix.1.copy_to(&mut self.0);
        }
        node.value.1.copy_to(&mut self.0);
        node.children.1.copy_to(&mut self.0);
        Ok(())
    }

    fn push_shortened_converted<S2: BlobStore>(
        &mut self,
        node: &TreeNode<'_, S2>,
        store: &S2,
        n: usize,
    ) -> Result<(), S2::Error> {
        /// TODO: get rid of this!
        let t: &mut OwnedNodeSeq<S2> = unsafe { std::mem::transmute(self) };
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

impl<S: BlobStore> AsRef<NodeSeq<S>> for OwnedNodeSeq<S> {
    fn as_ref(&self) -> &NodeSeq<S> {
        NodeSeq::new(self.0.as_ref())
    }
}

impl<S: BlobStore> Deref for OwnedNodeSeq<S> {
    type Target = NodeSeq<S>;

    fn deref(&self) -> &Self::Target {
        NodeSeq::new(self.0.as_ref())
    }
}

impl<S: BlobStore> Drop for OwnedNodeSeq<S> {
    fn drop(&mut self) {
        for elem in self.as_ref().iter() {
            if let Ok(elem) = elem {
                elem.drop();
            }
        }
        self.0.truncate(0);
    }
}

#[derive(Debug)]
struct Tree<S: BlobStore = NoStore> {
    /// This contains exactly one node, even in the case of an empty tree
    node: OwnedNodeSeq<S>,
    /// The associated store
    store: S,
}

impl Tree {
    fn empty() -> Self {
        Self {
            node: OwnedNodeSeq::empty_tree(),
            store: NoStore,
        }
    }

    fn single(key: &[u8], value: &[u8]) -> Self {
        Self {
            node: OwnedNodeSeq::single(key, value),
            store: NoStore,
        }
    }
}

impl Tree {
    pub fn outer_combine(
        &self,
        that: &Tree,
        f: impl Fn(&TreeValue, &TreeValue) -> Option<OwnedSlice<u8>> + Copy,
    ) -> Tree {
        unwrap_safe(self.try_outer_combine::<NoStore, NoError, _>(that, |a, b| Ok(f(a, b))))
    }
}

impl<S: BlobStore> Tree<S> {
    fn node(&self) -> TreeNode<'_, S> {
        self.node.iter().next().unwrap().unwrap()
    }

    fn dump(&self) -> Result<(), S::Error> {
        self.node().dump(0, &self.store)
    }

    pub fn try_outer_combine<S2, E, F>(&self, that: &Tree<S2>, f: F) -> Result<Tree, E>
    where
        S2: BlobStore,
        E: From<S::Error> + From<S2::Error> + From<NoError>,
        F: Fn(&TreeValue<S>, &TreeValue<S2>) -> Result<Option<OwnedSlice<u8>>, E> + Copy,
    {
        let mut nodes = OwnedNodeSeq::new();
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
    target: &mut OwnedNodeSeq,
) -> Result<(), E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValue<A>, &TreeValue<B>) -> Result<Option<OwnedSlice<u8>>, E> + Copy,
{
    let ap = a.prefix.load(ab)?;
    let bp = b.prefix.load(bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    let av = || a.value.load(ab);
    let bv = || b.value.load(bb);
    let prefix = OwnedSlice::from_slice(&ap[..n]);
    let mut children: OwnedNodeSeq = OwnedNodeSeq::new();
    let value: Option<OwnedSlice<u8>>;
    if n == ap.len() && n == bp.len() {
        // prefixes are identical
        value = if a.value.is_none() {
            if b.value.is_none() {
                // both none - none
                None
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
                f(&a.value, &b.value)?
            }
        };
        let ac = a.children.load(ab)?;
        let bc = b.children.load(bb)?;
        children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        value = av()?;
        let ac = a.children.load(ab)?;
        let bc = OwnedNodeSeq::shortened(b, bb, n)?;
        children = outer_combine_children(ac.iter(), &ab, bc.iter(), &bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        value = bv()?;
        let ac = OwnedNodeSeq::shortened(a, ab, n)?;
        let bc = b.children.load(bb)?;
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
) -> Result<OwnedNodeSeq, E>
where
    A: BlobStore,
    B: BlobStore,
    E: From<A::Error> + From<B::Error> + From<NoError>,
    F: Fn(&TreeValue<A>, &TreeValue<B>) -> Result<Option<OwnedSlice<u8>>, E> + Copy,
{
    let mut res = OwnedNodeSeq::new();
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

struct OuterJoin<'a, A: BlobStore, B: BlobStore, E> {
    a: TreeChildrenIterator<'a, A>,
    b: TreeChildrenIterator<'a, B>,
    p: PhantomData<E>,
}

// all this just so I could avoid having this expression twice in the iterator.
// Sometimes making things DRY in rust is hard...
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {        
        let a = Tree::single(b"aaaa", b"b");
        let b = Tree::single(b"aa", b"d");
        let r = a.outer_combine(&b, |a, b| Some(OwnedSlice::empty()));
        println!("{:?}", r);
        println!("{:?}", r.node.iter().next().unwrap().unwrap());
    
        let mut t = Tree::empty();
        for i in 0u64..1000 {
            let txt = i.to_string();
            let b = txt.as_bytes();
            t = t.outer_combine(&Tree::single(b, b), |a, b| Some(OwnedSlice::empty()))
        }
        println!("{:?}", t);
        println!("{:?}", t.node.iter().next().unwrap().unwrap());
        t.dump().unwrap();
    }
}
