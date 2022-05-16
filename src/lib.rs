mod iterators;
mod merge_state;
pub mod node;
pub mod store;
pub use node::Tree;
use node::{TreeNode, TreeValue};
use store::BlobStore;

#[cfg(test)]
#[macro_use]
extern crate maplit;

/// Utility to output something as hex
struct Hex<'a>(&'a [u8], usize);

impl<'a> Hex<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self(data, data.len())
    }
    fn partial(data: &'a [u8], len: usize) -> Self {
        let display = if len < data.len() { &data[..len] } else { data };
        Self(display, data.len())
    }
}

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() < self.1 {
            write!(f, "[{}..., {} bytes]", hex::encode(self.0), self.1)
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", hex::encode(self.0))
    }
}

pub trait NodeConverter<A: BlobStore, B: BlobStore>: Copy {
    fn convert_node(&self, node: &TreeNode<A>, store: &A) -> Result<TreeNode<B>, A::Error> {
        Ok(node.detached(store)?.downcast())
    }
    fn convert_value(&self, value: &TreeValue<A>, store: &A) -> Result<TreeValue<B>, A::Error> {
        Ok(value.detached(store)?.downcast())
    }
    fn convert_node_shortened(
        &self,
        node: &TreeNode<A>,
        store: &A,
        n: usize,
    ) -> Result<TreeNode<B>, A::Error> {
        let node = node.clone_shortened(store, n)?;
        self.convert_node(&node, store)
    }
}
