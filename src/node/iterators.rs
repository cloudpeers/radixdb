use std::marker::PhantomData;

use crate::{
    store::{BlobStore2 as BlobStore, OwnedBlob},
    Hex,
};

use super::refs::{self, TreeNode, TreePrefixRef};

pub(crate) struct OwnedNodeSeqIter<S: BlobStore>(OwnedBlob, usize, PhantomData<S>);

impl<S: BlobStore> OwnedNodeSeqIter<S> {
    pub fn new(data: OwnedBlob) -> Self {
        Self(data, 0, PhantomData)
    }

    pub fn next_node(&mut self) -> Option<TreeNode<'_, S>> {
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

    pub fn next_value_and_node(&mut self) -> Option<(Option<OwnedBlob>, TreeNode<'_, S>)> {
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
pub(crate) struct NodeSeqIter<'a, S>(&'a [u8], PhantomData<S>);

impl<'a, S: BlobStore> Clone for NodeSeqIter<'a, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<'a, S: BlobStore> Copy for NodeSeqIter<'a, S> {}

impl<'a, S: BlobStore> NodeSeqIter<'a, S> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data, PhantomData)
    }

    pub fn peek(&self) -> Option<Option<u8>> {
        TreePrefixRef::<S>::read(&self.0).map(|x| x.first_opt())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn next(&mut self) -> Option<TreeNode<'_, S>> {
        match TreeNode::read_one(&mut self.0) {
            Some((res, rest)) => {
                self.0 = rest;
                Some(res)
            }
            None => None,
        }
    }
}

impl<'a, S: BlobStore> Iterator for NodeSeqIter<'a, S> {
    type Item = TreeNode<'a, S>;

    fn next(&mut self) -> Option<Self::Item> {
        match TreeNode::read_one(&mut self.0) {
            Some((res, rest)) => {
                self.0 = rest;
                Some(res)
            }
            None => None,
        }
    }
}

pub(crate) struct FlexRefIter<'a>(&'a [u8]);

impl<'a> FlexRefIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> Iterator for FlexRefIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            let len = 1 + refs::len(self.0[0]);
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
