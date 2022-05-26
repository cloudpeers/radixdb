use std::{borrow::Borrow, ops::Deref, sync::Arc};

/// A blob backed by a dynamic blob owner
#[derive(Debug, Clone)]
pub struct Blob {
    /// The blob owner
    owner: Arc<dyn BlobOwner>,
    /// Extra data to allow a single BlobOwner to hand out multiple Blob s. E.g. this could be an offset within a page of shared memory.
    ///
    /// Make this a u64?
    extra: usize,
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Blob {
    fn as_slice(&self) -> &[u8] {
        self.owner.get_slice(self.extra)
    }

    /// Create a blob from a slice. This will allocate an `Arc<Vec<u8>>`.
    pub fn from_slice(data: &[u8]) -> Self {
        let owner: Arc<dyn BlobOwner> = Arc::new(data.to_vec());
        Self { owner, extra: 0 }
    }

    /// Create a new blob with a given BlobOwner and extra
    pub fn new(owner: Arc<dyn BlobOwner>, extra: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(owner.validate(extra));
        Ok(Self { owner, extra })
    }

    pub fn empty() -> Self {
        // todo static empty blob?
        Self::from_slice(&[])
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for Blob {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Borrow<[u8]> for Blob {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Trait for a blob owner
///
/// A blob owner can own a single blob, or it can be a "Page" containing multiple blobs, identified by the id
pub trait BlobOwner: Send + Sync + std::fmt::Debug + 'static {
    /// Called when a blob is being cloned
    fn validate(&self, _extra: usize) -> bool {
        true
    }
    /// Given an extra, get a slice
    fn get_slice(&self, extra: usize) -> &[u8];
}

/// Implementation of BlobOwner for Vec<u8>, so an `Arc<Vec<u8>>` can be used as an Arc<dyn BlobOwner>
impl BlobOwner for Vec<u8> {
    fn get_slice(&self, _: usize) -> &[u8] {
        self.as_ref()
    }

    fn validate(&self, _: usize) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::Blob;
    use proptest::prelude::*;
    use std::{borrow::Borrow, ops::Deref};

    #[test]
    fn size() {
        #[cfg(target_pointer_width = "64")]
        assert_eq!(std::mem::size_of::<Blob>(), 24);
        #[cfg(target_pointer_width = "32")]
        assert_eq!(std::mem::size_of::<Blob>(), 12);
    }

    proptest! {
        #[test]
        fn from_slice_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..24)) {
            let blob = Blob::from_slice(&data);
            prop_assert_eq!(&data, blob.as_ref());
            prop_assert_eq!(&data, blob.deref());
            prop_assert_eq!(&data, Borrow::<[u8]>::borrow(&blob));
        }
    }
}
