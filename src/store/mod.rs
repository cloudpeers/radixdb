//! The blob and blob store trait and implementation.
//!
//! You only have to interact with these types if you want to use RadixTrees as persistent databases.
mod blob;
pub(crate) mod blob_store;
#[cfg(feature = "mem-store")]
mod mem_store;
#[cfg(all(not(target_arch = "wasm32"), feature = "custom-store"))]
mod paged_file_store;

#[cfg(feature = "custom-store")]
pub use blob_store::DynBlobStore;
#[cfg(feature = "custom-store")]
pub use blob_store::UnwrapSafeExt;
pub use blob_store::{Blob, BlobStore, Detached, NoError};

#[cfg(feature = "mem-store")]
pub use mem_store::MemStore;

#[cfg(all(not(target_arch = "wasm32"), feature = "paged-file-store"))]
pub use paged_file_store::PagedFileStore;
