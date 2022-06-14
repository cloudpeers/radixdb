#[cfg(test)]
mod tests {
    use crate::store::Blob;
    use proptest::prelude::*;
    use std::{borrow::Borrow, ops::Deref};

    #[test]
    fn size() {
        #[cfg(target_pointer_width = "64")]
        assert_eq!(std::mem::size_of::<Blob>(), 32);
        #[cfg(target_pointer_width = "32")]
        assert_eq!(std::mem::size_of::<Blob>(), 16);
    }

    proptest! {
        #[test]
        fn from_slice_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..24)) {
            let blob = Blob::copy_from_slice(&data);
            prop_assert_eq!(&data, blob.as_ref());
            prop_assert_eq!(&data, blob.deref());
            prop_assert_eq!(&data, Borrow::<[u8]>::borrow(&blob));
        }
    }
}
