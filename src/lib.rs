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
//! ## Example
//!
//! ```rust
//! # use radixdb::*;
//! let mut dict = RadixTree::default();
//! dict.insert("dog", "Hund");
//! dict.insert("cat", "Katze");
//! assert!(dict.contains_key("dog"));
//! for (k, v) in dict.iter() {
//!   println!("{:?} {:?}", k, v);
//! }
//! ```
//!
//! # Advanced usage
//!
//! You can provide a custom store for a radix tree, which can be either a contiguous slice of memory, a file on disk, or a custom storage backend.
//! Custom storage backends are enabled using the `custom-storage` feature.
//!
//! The storage is usually fallible, e.g. when reading from a disk or network. When using a fallible storage, every interaction with a radix tree can fail.
//! Therefore there is a fallible version of all methods.
//!
//! ## Example
//!
//! ```rust
//! # use radixdb::*;
//! # use store::BlobStore;
//! # fn test() -> anyhow::Result<()> {
//! // build a small tree as above
//! let mut dict = RadixTree::default();
//! dict.insert("dog", "Hund");
//! dict.insert("cat", "Katze");
//! // attach it to a store - here we use a memstore, but typically you would use a file store
//! let store = store::MemStore::default();
//! let mut dict = dict.try_attached(store.clone())?;
//! // the store is fallible, so we have to use the try_... variants for interacting with it
//! assert!(dict.try_contains_key("cat")?);
//! for r in dict.try_iter() {
//!   if let Ok((k, v)) = r {
//!     println!("{:?} {:?}", k, v);
//!   }
//! }
//! // add another entry. This is now in memory
//! dict.try_insert("rabbit", "Hase")?;
//! // persist all changes to the store
//! dict.try_reattach()?;
//! // sync the store to disk
//! store.sync()?;
//! # Ok(())
//! # }
//! ```
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
