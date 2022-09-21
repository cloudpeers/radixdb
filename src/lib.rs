//! A radix tree data structure
//!
//! A radix tree is a map data structure that has fast and memory efficient storage of keys that have common prefixes.
//! It can be used as a set by using a unit value.
//!
//! This radix tree is using blobs as keys and values. A common use case is to use UTF-8 strings as keys.
//!
//! The lookup performance of this radix tree is roughly in the same range as a [std::collections::BTreeMap].
//!
//! Where it shines is when you have long keys that frequently have a common prefix, like e.g. namespaced identifiers.
//! In that case it will use much less memory than storing the keys as a whole.
//!
//! It will also be extremely fast in selecting a subtree given a prefix, like for example for autocompletion.
//!
//! Elements in a radix tree as lexicographically sorted.
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
//! # Bulk operations
//!
//! RadixTree supports bulk operations. These are modeled somewhat after database relational operations. Bulk operations take a combiner function to specify
//! how to handle collisions.
//!
//! Bulk operations are much faster than adding elements one by one, especially if the two radix trees are disjoint.
//!
//! ## Example
//!
//! ```rust
//! # use radixdb::*;
//! // use the radixtree macro to create a set of strings. This works because &str implements AsRef<[u8]>.
//! let mut collections = radixtree! {
//!   "std::collections::BTreeMap",
//!   "std::collections::BTreeSet",
//!   "std::collections::HashMap",
//!   "std::collections::HashSet",
//! };
//! // create another set that is disjoint
//! let formatting = radixtree! {
//!   "std::fmt::Debug",
//!   "std::fmt::Display",
//! };
//! // in place union of the two sets
//! let mut all = collections;
//! all.outer_combine_with(&formatting, |a, b| {});
//! // find identifiers, e.g. for autocompletion
//! for (k, _) in all.scan_prefix("std::c") {
//!   println!("{:?}", k);
//! }
//! ```
//!
//! # Using a custom blob storage
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
//!
//! # Data format
//!
//! Radix trees can be very quickly serialized and deserialized. Using a custom store, they can also be traversed and queried
//! without deserializing at all. This is useful to query a very large tree that does not fit into memory.
//!
//! ## Serialized format
//!
//! Each tree node consists of prefix, optional value, and optional children.
//! Either value or children must be defined for a valid node.
//! An empty prefix is only allowed for the root node.
//! Prefix and value can be stored either directly or indirectly via an id. Children is so far only stored as id.
//!
//! Prefix, value and children are each serialized as a header byte followed by up to 127 value bytes.
//!
//! ### Header byte format
//!
//! The highest bit of the header byte is used to distinguish between inline value (0) and id value (1). The lower
//! 7 bits are the length.
//!
//! An id with a length of `0` is used as a special value for `None`. This is only valid for value and children.
//! Likewise, data with the length of `0` is used to store the empty array.
//!
//! Since the longest possible length is 127, data longer than 127 bytes must always be stored indirectly.
//!
//! ### Prefix value
//!
//! When a prefix is stored indirectly, an extra byte is used to store the first byte of the prefix.
//!
//! ### Examples
//!
//! #### Simplest possible node
//!
//! ```ignore
//! { "" => "" }
//!
//! 000080
//! 00    | prefix: 0 byte long literal ""
//!   00  | value: 0 byte long literal ""
//!     80| children: None
//! ```
//!
//! #### Small leaf node
//!
//! Small values are stored inline
//!
//! ```ignore
//! { "hello" => "world" }
//!
//! 0568656c6c6f06776f726c642180
//! 0568656c6c6f                | prefix: 5 byte long literal "hello"
//!             06776f726c6421  | value: 6 byte long literal "world!"
//!                           80| children: None
//! ```
//!
//! #### Large leaf node
//!
//! Large values above 127 bytes must be stored indirectly.
//! The first byte of the prefix is prepended to the id to simplify searching.
//! For the value, just the id is stored.
//!
//! ```ignore
//! { [b'a'; 256] => [b'b'; 256] }
//!
//! 8961000000000000000188000000000000000280
//! 89610000000000000001                    | prefix: first char b"a", 8 byte long id
//!                     880000000000000002  | value: 8 byte long id
//!                                       80| no children
//! ```
//!
//! #### Branch node
//!
//! For a branch node, childfen are always stored indirectly.
//! A record size byte is added to simplify indexing. The record size is set to
//!
//! ```ignore
//! { "aa" => "", "ab" => "" }
//!
//! 01618089040000000000000001
//! 8161                      | prefix: 1 byte long literal "a"
//!     80                    | value: None
//!       89040000000000000001| children: constant record size 4u8, 8 byte long id
//! ```
//!
//! The child id refers to a block of data that contains the 2 children. Both children have a size of 4 bytes.
//!
//! ```ignore
//! 0161008001620080
//! 0161            | prefix: 1 byte long literal "a"
//!     00          | value: 0 byte long literal ""
//!       80        | children: None
//!         0162    | prefix: 1 byte long literal "b"
//!             00  | value: 0 byte long literal ""
//!               80| children: None
//! ```
pub mod node;
pub mod store;
mod util;
use node::TreeNode;
use store::{BlobStore, Detached};
use util::{Hex, Lit};

#[cfg(test)]
mod tests;

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
/// Elements must implement `AsRef<[u8]>`, so both `&str` and `&[u8]` work.
///
/// # Map example:
/// ```rust
/// let en_de = radixdb::radixtree! {
///   "dog" => "Hund",
///   "cat" => "Katze",
/// };
/// ```
///
/// # Set example:
/// ```rust
/// let latin = radixdb::radixtree! {
///   "romane",
///   "romanus",
///   "romulus",
///   "rubens",
///   "ruber",
///   "rubicon",
///   "rubicundus",
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
            _elems.into_iter().collect::<$crate::RadixTree>()
        }
    };

    // set, trailing comma case
    ($($key:expr,)+) => (radixtree!($($key),+));

    ( $($key:expr),* ) => {
        {
            let mut _elems = ::std::vec::Vec::<(&[u8], &[u8])>::new();
            $(
                let _ = _elems.push(($key.as_ref(), "".as_ref()));
            )*
            _elems.into_iter().collect::<$crate::RadixTree>()
        }
    };
}
