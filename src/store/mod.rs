//! The blob and blob store trait and implementation.
//!
//! You only have to interact with these types if you want to use RadixTrees as persistent databases.
mod blob;
mod blob_store;
#[cfg(not(target_arch = "wasm32"))]
mod paged_file_store;

pub use blob_store::{
    Blob, BlobStore, DynBlobStore, MemStore, NoError, NoStore, OwnedBlob, UnwrapSafeExt,
};

#[cfg(not(target_arch = "wasm32"))]
pub use paged_file_store::PagedFileStore;
