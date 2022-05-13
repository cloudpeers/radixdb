use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use std::ops::Deref;

use crate::{node::FlexRef, slice_cast, store::Blob};

#[derive(Debug, Clone)]
pub enum OwnedSlice<T> {
    /// an owned slice that comes from a blob store
    Blob { blob: Blob, p: PhantomData<T> },
    /// an detached owned slice backed by a FlexRef (no id!)
    Flex { flex: FlexRef<Vec<T>> },
}

impl<T> OwnedSlice<T> {
    pub const fn empty() -> Self {
        Self::Flex {
            flex: FlexRef::INLINE_EMPTY_ARRAY,
        }
    }

    pub fn from_slice(data: &[T]) -> Self
    where
        T: Clone,
    {
        Self::Flex {
            flex: FlexRef::owned_from_arc(Arc::new(data.to_vec())),
        }
    }

    pub fn from_arc_vec(data: Arc<Vec<T>>) -> Self
    where
        T: Clone,
    {
        Self::Flex {
            flex: FlexRef::owned_from_arc(data),
        }
    }

    pub const fn blob(blob: Blob) -> Self {
        Self::Blob {
            blob,
            p: PhantomData,
        }
    }

    pub const fn flex(flex: FlexRef<Vec<T>>) -> Self {
        assert!(flex.is_inline() || flex.is_arc());
        Self::Flex { flex }
    }

    pub fn cast<U>(&self) -> anyhow::Result<OwnedSlice<U>> {
        match self {
            Self::Blob { blob, .. } => {
                let _ = slice_cast::<u8, U>(blob.as_ref())?;
                Ok(OwnedSlice::Blob {
                    blob: blob.clone(),
                    p: PhantomData,
                })
            }
            Self::Flex { .. } => {
                anyhow::bail!("not allowed");
            }
        }
    }

    fn as_slice(&self) -> &[T] {
        match self {
            Self::Flex { flex } => {
                if let Some(x) = flex.owned_arc_ref() {
                    x.as_ref()
                } else if let Some(x) = flex.inline_as_ref() {
                    if x.len() == 0 {
                        &[]
                    } else {
                        slice_cast::<u8, T>(x).unwrap()
                    }
                } else {
                    panic!()
                }
            }
            Self::Blob { blob, .. } => slice_cast::<u8, T>(blob.as_ref()).unwrap(),
        }
    }
}

impl<T> AsRef<[T]> for OwnedSlice<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Deref for OwnedSlice<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> Borrow<[T]> for OwnedSlice<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: PartialEq> PartialEq for OwnedSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, ops::Deref};

    use proptest::prelude::*;

    use crate::{node::FlexRef, store::Blob, OwnedSlice};

    #[test]
    fn size() {
        #[cfg(target_pointer_width = "64")]
        assert_eq!(std::mem::size_of::<OwnedSlice<u8>>(), 32);
        #[cfg(target_pointer_width = "32")]
        assert_eq!(std::mem::size_of::<OwnedSlice<u8>>(), 16);
    }

    proptest! {
        #[test]
        fn from_slice_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..24)) {
            let blob = Blob::from_slice(&data);
            prop_assert_eq!(&data, blob.as_ref());
            prop_assert_eq!(&data, blob.deref());
            prop_assert_eq!(&data, Borrow::<[u8]>::borrow(&blob));
        }

        #[test]
        fn inline(data in proptest::collection::vec(any::<u8>(), 0..8)) {
            if let Some(blob) = FlexRef::inline_from_slice(&data).map(OwnedSlice::flex) {
                prop_assert_eq!(&data, blob.as_ref());
                prop_assert!(data.len() <= 6);
            } else {
                prop_assert!(data.len() > 6);
            }
        }

        #[test]
        fn no_cast_typed(data in proptest::collection::vec(any::<u32>(), 0..24)) {
            let blob = OwnedSlice::from_slice(&data);
            prop_assert!(blob.cast::<u8>().is_err())
        }

        #[test]
        fn no_cast_inline(number in any::<u32>()) {
            let bytes = number.to_be_bytes();
            let blob = OwnedSlice::from_slice(&bytes);
            prop_assert!(blob.cast::<u32>().is_err())
        }

        // #[test]
        // fn cast_arc_u8(bytes in any::<[u8; 8]>()) {
        //     let blob = OwnedSlice::from_slice(bytes.as_ref().into());
        //     let blob = blob.cast::<[u8; 8]>().unwrap();
        //     prop_assert!(blob.as_ref()[0] == bytes);
        // }
    }
}
