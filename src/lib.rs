//! A radix tree data structure
//!
//! A radix tree is a map data structure that has fast and memory efficient storage of keys that have common prefixes.
//! It can be used as a set by using a unit value.
//!
//! This radix tree is using blobs as keys and values. A common use case is to use UTF-8 strings as keys,
//!
//! # Basic usage
//!
//! Basic usage is not that different from using a std collection such as BTreeMap.
//!
//! # Advanced usage
//!
//! You can provide a custom store for a radix tree, which can be either a contiguous slice of memory, a file on disk, or a custom storage backend.
//! Custom storage backends are enabled using the `custom-storage` feature.
//!
//! The storage can be fallible, e.g. when reading from a disk or network. That is enabled using the `fallible-storage` feature.
pub mod node;
pub mod store;
use node::TreeNode;
use store::{BlobStore, NoStore};

#[cfg(test)]
#[macro_use]
extern crate maplit;

/// A radix tree
#[derive(Debug, Clone)]
pub struct RadixTree<S: BlobStore = NoStore> {
    node: TreeNode<S>,
    /// The associated store
    store: S,
}

struct Lit(String);

impl std::fmt::Debug for Lit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> std::fmt::Display for Lit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Utility to output something as hex
struct Hex<'a>(&'a [u8], usize);

impl<'a> Hex<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self(data, data.len())
    }

    #[allow(dead_code)]
    fn partial(data: &'a [u8], len: usize) -> Self {
        Self(data, len)
    }
}

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() > self.1 {
            write!(
                f,
                "[{}..., {} bytes]",
                hex::encode(&self.0[..self.1]),
                self.0.len()
            )
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() > self.1 {
            write!(
                f,
                "[{}..., {} bytes]",
                hex::encode(&self.0[..self.1]),
                self.0.len()
            )
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}
