use std::{cmp::Ordering, marker::PhantomData, sync::Arc};

use crate::store::{
    unwrap_safe, Blob2 as Blob, BlobStore2 as BlobStore, NoError, NoStore, OwnedBlob,
};

use super::{
    builders::{AtChildren, AtPrefix, InPlaceBuilderRef, InPlaceNodeSeqBuilder, NodeSeqBuilder},
    iterators::{NodeSeqIter, OwnedNodeSeqIter},
    refs::{TreeNode, TreeValueRef, TreeValueRefWrapper},
};

#[derive(Debug, Clone)]
pub struct Tree<S: BlobStore = NoStore> {
    /// This contains exactly one node, even in the case of an empty tree
    node: Arc<NodeSeqBuilder<S>>,
    /// The associated store
    store: S,
}

impl<S: BlobStore + Default> Default for Tree<S> {
    fn default() -> Self {
        Self::empty(S::default())
    }
}

impl Tree {
    pub fn single(key: &[u8], value: &[u8]) -> Self {
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

    pub fn try_attached<S: BlobStore>(&self, store: S) -> Result<Tree<S>, S::Error> {
        let mut t: NodeSeqBuilder<S> = unsafe { std::mem::transmute(self.node.as_ref().clone()) };
        let mut ip = InPlaceNodeSeqBuilder::new(&mut t);
        ip.attach(&store)?;
        Ok(Tree {
            node: Arc::new(ip.into_inner()),
            store,
        })
    }
}

impl<S: BlobStore> Tree<S> {
    fn node(&self) -> TreeNode<'_, S> {
        TreeNode::read(&self.node.data).unwrap()
    }

    pub fn empty(store: S) -> Self {
        Self::new(NodeSeqBuilder::empty(), store)
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
    pub fn try_get(&self, key: &[u8]) -> Result<Option<TreeValueRefWrapper<S>>, S::Error> {
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
                        .map(|x| TreeValueRefWrapper::new(o.slice_ref(x.bytes())))
                } else {
                    None
                })
            },
        )
    }

    /// True if key is contained in this set
    pub fn try_contains_key(&self, key: &[u8]) -> Result<bool, S::Error> {
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
        let mut nodes = NodeSeqBuilder::empty();
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
        let mut nodes = NodeSeqBuilder::empty();
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
        let mut nodes = NodeSeqBuilder::empty();
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
        self.node().first_entry(&self.store, prefix.to_vec())
    }

    pub fn try_last_entry(
        &self,
        prefix: &[u8],
    ) -> Result<Option<(OwnedBlob, TreeValueRefWrapper<S>)>, S::Error> {
        self.node().last_entry(&self.store, prefix.to_vec())
    }

    pub fn try_filter_prefix<'a>(&'a self, prefix: &[u8]) -> Result<Tree<S>, S::Error> {
        filter_prefix(&self.node(), &self.owned_blob(), &self.store, prefix)
            .map(|node| Tree::new(node, self.store.clone()))
    }

    pub fn try_reattach(&mut self, store: S) -> Result<(), S::Error> {
        let mut t: NodeSeqBuilder<S> = self.node.as_ref().clone();
        let mut ip = InPlaceNodeSeqBuilder::new(&mut t);
        ip.attach(&store)?;
        self.node = Arc::new(ip.into_inner());
        Ok(())
    }

    pub fn try_detached(&self, store: S) -> Result<Tree, S::Error> {
        let mut t: NodeSeqBuilder<S> = self.node.as_ref().clone();
        let mut ip = InPlaceNodeSeqBuilder::new(&mut t);
        ip.detach(&store)?;
        let b = ip.into_inner();
        let node: NodeSeqBuilder<NoStore> = unsafe { std::mem::transmute(b) };
        Ok(Tree {
            node: Arc::new(node),
            store: NoStore,
        })
    }

    pub fn try_validate(&self, store: &S) -> Result<bool, S::Error> {
        self.node().validate(store)
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
    let mut children: NodeSeqBuilder = NodeSeqBuilder::empty();
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
    let mut res = NodeSeqBuilder::empty();
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
        let a = match (a.peek().value_opt(), b.value().value_opt()) {
            (Some(av), Some(bv)) => {
                let r = f(av, bv)?;
                a.push_value_opt(r)
            }
            (Some(_), None) => a.move_value(),
            (None, _) => a.push_converted(b.value(), &bb)?,
        };
        let bc = b.children().load(&bb)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == ap.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        let a = a.move_prefix().move_value();
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        outer_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::empty();
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
        let mut child = NodeSeqBuilder::<A>::empty();
        let cursor = child.cursor();
        let cursor = cursor.push_prefix(&ap[n..]);
        let a = a.move_prefix_shortened(n, &ab)?;
        let cursor = cursor.push_value_raw(a.peek());
        let a = a.push_value_none();
        let _ = cursor.push_children_raw(a.peek());
        let mut children = NodeSeqBuilder::<A>::empty();
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
        let mut res = NodeSeqBuilder::empty();
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
    let mut children: NodeSeqBuilder = NodeSeqBuilder::empty();
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
    let mut res = NodeSeqBuilder::empty();
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
        let a = match (a.peek().value_opt(), b.value().value_opt()) {
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
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        inner_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::empty();
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
    let ap = a.prefix().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        if let (Some(av), Some(bv)) = (a.value().value_opt(), b.value().value_opt()) {
            if f(av, bv)? {
                return Ok(true);
            }
        };
        let ac = a.children().load(&ab)?;
        let bc = b.children().load(&bb)?;
        inner_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == ap.len() {
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        let ac = a.children().load(&ab)?;
        inner_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == bp.len() {
        let mut ac = NodeSeqBuilder::<A>::empty();
        ac.push_shortened(a, &ab, n)?;
        let bc = b.children().load(&bb)?;
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
    let mut children: NodeSeqBuilder = NodeSeqBuilder::empty();
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
    let mut res = NodeSeqBuilder::empty();
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
    let ap = a.prefix().load(&ab)?;
    let bp = b.prefix().load(&bb)?;
    let n = common_prefix(ap.as_ref(), bp.as_ref());
    if n == ap.len() && n == bp.len() {
        match (a.value().value_opt(), b.value().value_opt()) {
            (Some(av), Some(bv)) => {
                if f(av, bv)? {
                    return Ok(true);
                }
            }
            (Some(_), None) => return Ok(true),
            _ => {}
        };
        let ac = a.children().load(&ab)?;
        let bc = b.children().load(&bb)?;
        left_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == ap.len() {
        if a.value().is_some() {
            return Ok(true);
        };
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        let ac = a.children().load(&ab)?;
        left_combine_children_pred(ac.iter(), ab, bc.iter(), bb, f)
    } else if n == bp.len() {
        let mut ac = NodeSeqBuilder::<A>::empty();
        ac.push_shortened(a, &ab, n)?;
        let bc = b.children().load(&bb)?;
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
        let a = if let (Some(av), Some(bv)) = (a.peek().value_opt(), b.value().value_opt()) {
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
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        left_combine_children_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // b is a prefix of a
        // value is value of b
        // split a at n
        let mut child = NodeSeqBuilder::<A>::empty();
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
        let mut bc = NodeSeqBuilder::<B>::empty();
        bc.push_shortened(b, &bb, n)?;
        retain_children_prefix_with(a, ab, bc.iter(), bb, f)?;
    } else if n == bp.len() {
        // a is a prefix of b
        // value is value of a
        // move prefix and value
        if b.value().is_none() {
            // split a at n
            let mut child = NodeSeqBuilder::<A>::empty();
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
        let mut bc = NodeSeqBuilder::<B>::empty();
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
            let mut child = NodeSeqBuilder::<A>::empty();
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
            return find(store, tree_children.blob(), &child, &prefix[n..], f);
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
                let mut t = NodeSeqBuilder::empty();
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
                let mut t = NodeSeqBuilder::empty();
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

/// Return the subtree with the given prefix. Will return an empty tree in case there is no match.
fn filter_prefix<S: BlobStore>(
    node: &TreeNode<S>,
    owner: &OwnedBlob,
    store: &S,
    prefix: &[u8],
) -> Result<NodeSeqBuilder<S>, S::Error> {
    find(store, owner, node, prefix, |_, x| {
        Ok(match x {
            FindResult::Found(res) => {
                let mut t = NodeSeqBuilder::empty();
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
                let mut t = NodeSeqBuilder::empty();
                t.cursor()
                    .push_prefix(rp)
                    .push_value_raw(res.value())
                    .push_children_raw(res.children());
                t
            }
            FindResult::NotFound { .. } => NodeSeqBuilder::empty(),
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
                let prefix = node.prefix().load(&self.store)?;
                let prefix_len = prefix.len();
                let children = node.children().load_owned(&self.store)?.owned_iter();
                self.path.append(prefix.as_ref());
                self.stack.push((prefix_len, value, children));
            } else {
                if let Some(value) = self.top_value().take() {
                    let value = TreeValueRefWrapper::new(value);
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
                let prefix = node.prefix().load(&self.store)?;
                let prefix_len = prefix.len();
                self.path.append(prefix.as_ref());
                if (self.descend)(&self.path.0, &node) {
                    let children = node.children().load_owned(&self.store)?.owned_iter();
                    let res = if node.value().is_some() {
                        let mut t = NodeSeqBuilder::empty();
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
                    let mut res = NodeSeqBuilder::empty();
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
                let children = node.children().load_owned(&self.store)?.owned_iter();
                self.stack.push(children);
                if let Some(value) = value {
                    let value = TreeValueRefWrapper::new(value);
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
        let mut tree = Tree::default();
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

    use crate::store::{MemStore, MemStore2};

    use super::*;

    // fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
    //     proptest::strategy::Union::new_weighted(vec![
    //         (10, proptest::collection::vec(b'0'..b'9', 0..9)),
    //         (1, proptest::collection::vec(b'0'..b'9', 128..129)),
    //     ])
    // }

    // fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    //     proptest::strategy::Union::new_weighted(vec![
    //         (10, proptest::collection::vec(any::<u8>(), 0..9)),
    //         (1, proptest::collection::vec(any::<u8>(), 128..129)),
    //     ])
    // }

    fn arb_prefix() -> impl Strategy<Value = Vec<u8>> {
        proptest::strategy::Union::new_weighted(vec![
            (10, proptest::collection::vec(b'0'..b'9', 0..9)),
            (1, proptest::collection::vec(b'0'..b'9', 128..129)),
        ])
        // proptest::collection::vec(b'0'..b'9', 0..9)
    }

    fn arb_value() -> impl Strategy<Value = Vec<u8>> {
        // proptest::collection::vec(any::<u8>(), 0..2)
        proptest::strategy::Union::new_weighted(vec![
            (10, proptest::collection::vec(any::<u8>(), 0..9)),
            (1, proptest::collection::vec(any::<u8>(), 128..129)),
        ])
    }

    fn arb_tree_contents() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
        proptest::collection::btree_map(arb_prefix(), arb_value(), 0..10)
    }

    fn arb_owned_tree() -> impl Strategy<Value = Tree> {
        arb_tree_contents().prop_map(|x| mk_owned_tree(&x))
    }

    fn mk_owned_tree(v: &BTreeMap<Vec<u8>, Vec<u8>>) -> Tree {
        let tree: Tree = v.clone().into_iter().collect();
        if !tree.try_validate(&NoStore).unwrap() {
            println!("{:?}", v);
        }
        tree
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
                let prefix = tree.node().prefix().load(&NoStore).unwrap().to_vec();
                if prefix.len() <= n {
                    prop_assert!(tree.node().children().is_empty());
                    prop_assert!(tree.node().value().is_some());
                    let value = tree.node().value().load(&NoStore).unwrap().unwrap().to_vec();
                    let prev = leafs.insert(prefix, value);
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

        #[test]
        fn attach_detach_roundtrip(x in arb_tree_contents()) {
            let reference = x;
            let tree = mk_owned_tree(&reference);
            let store = MemStore2::default();
            let tree = tree.try_attached(store.clone()).unwrap();
            let tree = tree.try_detached(store).unwrap();
            let actual = to_btree_map(&tree);
            prop_assert_eq!(reference, actual);
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
    fn difference_with2() {
        let a = btreemap! { vec![0;130] => vec![] };
        let b = btreemap! { vec![0;129] => vec![] };
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
    fn long_prefix_1() {
        let a = btreemap! { vec![1] => vec![], vec![2; 129] => vec![] };
        let at = mk_owned_tree(&a);
        assert!(at.try_validate(&NoStore).unwrap());
    }

    #[test]
    fn long_prefix_2() {
        let a = btreemap! {
            vec![1, 1] => vec![],
            vec![1, 2] => vec![],
        };
        let b = btreemap! {
            vec![1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] => vec![]
        };
        let at = mk_owned_tree(&a);
        let bt = mk_owned_tree(&b);
        assert!(at.try_validate(&NoStore).unwrap());
        assert!(bt.try_validate(&NoStore).unwrap());
        let mut rt = at;
        rt.outer_combine_with(&bt, |a, b| Some(b.to_owned()));
        assert!(rt.try_validate(&NoStore).unwrap());
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
    fn attach_detach() {
        let elems = (0..1000u64)
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
        let tree: Tree = elems.into_iter().collect();
        let store = MemStore2::default();
        let tree = tree.try_attached(store.clone()).unwrap();
        for e in tree.try_iter() {
            let (k, v) = e.unwrap();
            println!("attached {:?} {:?}", k, v);
        }
        tree.dump().unwrap();
        let tree = tree.try_detached(store).unwrap();
        tree.dump().unwrap();
        for (k, v) in tree.iter() {
            println!("detached {:?} {:?}", k, v);
        }
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
    fn sizes() {
        println!("{}", std::mem::size_of::<TreeNode>());
        println!("{}", std::mem::size_of::<OwnedBlob>());
    }

    #[test]
    fn smoke() {
        let a = Tree::single(b"aaaa", b"b");
        let b = Tree::single(b"aa", b"d");
        let r = a.outer_combine(&b, |_, b| Some(b.to_owned()));
        println!("{:?}", r);
        println!("{:?}", r.node());

        let mut t = Tree::default();
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
        let mut t = Tree::default();
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
