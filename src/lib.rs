//! A radix tree data structure
//!
//! A radix tree is a map data structure that has fast and memory efficient storage of keys that have common prefixes.
//! It can be used as a set by using a unit value.
//!
//! This radix tree is using blobs as keys and values. A common use case is to use UTF-8 strings as keys.
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
//! The storage can be fallible, e.g. when reading from a disk or network.
pub mod node;
pub mod store;
mod util;
use node::TreeNode;
use store::{BlobStore, Detached};
use util::{Hex, Lit};

#[cfg(test)]
#[macro_use]
extern crate maplit;

/// A radix tree
#[derive(Debug, Clone)]
pub struct RadixTree<S: BlobStore = Detached> {
    node: TreeNode<S>,
    /// The associated store
    store: S,
}

/// A macro to generate a radix tree from key value pairs, similar to the [maplit](https://docs.rs/maplit/1.0.2/maplit/) crate.
///
/// # Example:
/// ```rust
/// let tree = radixdb::radixtree! {
///   b"romane" => b"",
///   b"romanus" => b"",
///   b"romulus" => b"",
///   b"rubens" => b"",
///   b"ruber" => b"",
///   b"rubicon" => b"",
///   b"rubicundus" => b"",
/// };
/// ```
#[macro_export(local_inner_macros)]
macro_rules! radixtree {
    // trailing comma case
    ($($key:expr => $value:expr,)+) => (radixtree!($($key => $value),+));

    ( $($key:expr => $value:expr),* ) => {
        {
            let mut _elems = ::std::vec::Vec::<(&[u8], &[u8])>::new();
            $(
                let _ = _elems.push(($key.as_ref(), $value.as_ref()));
            )*
            _elems.into_iter().collect::<::radixdb::RadixTree>()
        }
    };
}
