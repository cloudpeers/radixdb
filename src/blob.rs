use crate::{slice_cast, slice_cast_raw};
use std::{borrow::Borrow, marker::PhantomData, ops::Deref, sync::Arc};

#[repr(C)]
#[derive(Debug)]
pub enum Blob<T> {
    /// data is stored inline. Drop will do nothing to the content.
    Inline {
        data: [u8; 16],
        len: u8,
        p: PhantomData<T>,
    },
    /// we got the data in an actual Arc<Vec<T>>. Drop will be handled properly.
    ArcVecT { arc: Arc<Vec<T>> },
    /// we got the data in an Arc<[u8]>. Drop will do nothing to the content.
    ArcVecU8 { arc: Arc<[u8]>, p: PhantomData<T> },
    /// custom way to keep the data alive. Drop will do nothing to the content.
    Custom {
        owner: Arc<dyn BlobOwner>,
        extra: usize,
        p: PhantomData<T>,
    },
}

impl<T> Clone for Blob<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Inline { len, data, p } => Self::Inline {
                len: len.clone(),
                data: data.clone(),
                p: p.clone(),
            },
            Self::ArcVecT { arc } => Self::ArcVecT { arc: arc.clone() },
            Self::ArcVecU8 { arc, p } => Self::ArcVecU8 {
                arc: arc.clone(),
                p: *p,
            },
            Self::Custom { owner, extra, p } => {
                owner.inc(*extra);
                Self::Custom {
                    owner: owner.clone(),
                    extra: *extra,
                    p: *p,
                }
            }
        }
    }
}

impl<T: PartialEq> PartialEq for Blob<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T> Drop for Blob<T> {
    fn drop(&mut self) {
        if let Self::Custom { owner, extra, .. } = self {
            owner.dec(*extra);
        }
    }
}

pub trait BlobOwner: Send + Sync + std::fmt::Debug + 'static {
    fn inc(&self, _extra: usize) {}
    fn dec(&self, _extra: usize) {}
    fn get_slice(&self, extra: usize) -> &[u8];
}

impl Blob<u8> {
    pub fn arc_from_byte_slice(slice: &[u8]) -> Self {
        if let Some(res) = Self::inline(slice) {
            res
        } else {
            Self::arc_u8(slice.into())
        }
    }
}

impl<T> Blob<T> {
    pub fn cast<U>(&self) -> anyhow::Result<Blob<U>> {
        match self {
            Self::Inline { .. } => Err(anyhow::anyhow!("casting not supported for inline")),
            Self::ArcVecT { .. } => Err(anyhow::anyhow!("casting not supported for arc")),
            Self::Custom { owner, extra, .. } => {
                let _ = slice_cast::<u8, U>(owner.get_slice(*extra))?;
                owner.inc(*extra);
                Ok(Blob::Custom {
                    owner: owner.clone(),
                    extra: *extra,
                    p: PhantomData,
                })
            }
            Self::ArcVecU8 { arc, .. } => {
                let _ = slice_cast::<u8, U>(arc.as_ref().as_ref())?;
                Ok(Blob::ArcVecU8 {
                    arc: arc.clone(),
                    p: PhantomData,
                })
            }
        }
    }

    pub fn arc_vec_t(arc: Arc<Vec<T>>) -> Self {
        Self::ArcVecT { arc }
    }

    pub fn arc_u8(arc: Arc<[u8]>) -> Self {
        Self::ArcVecU8 {
            arc,
            p: PhantomData,
        }
    }

    pub fn custom(cbs: Arc<dyn BlobOwner>, extra: usize) -> Self {
        cbs.inc(extra);
        Self::Custom {
            owner: cbs,
            extra,
            p: PhantomData,
        }
    }

    pub fn from_slice(slice: &[T]) -> Self
    where
        T: Clone,
    {
        if let Some(res) = Self::inline(slice) {
            res
        } else {
            Self::arc_vec_t(Arc::new(slice.to_vec()))
        }
    }

    pub fn inline(slice: &[T]) -> Option<Self> {
        let bytes = slice_cast::<T, u8>(slice).unwrap();
        let len = bytes.len();
        if len == 0 {
            let data = [0u8; 16];
            Some(Self::Inline {
                len: 0,
                data,
                p: PhantomData,
            })
        } else if len <= 16 && std::mem::size_of::<T>() == 1 {
            let mut data = [0u8; 16];
            data[0..len].copy_from_slice(bytes);
            Some(Self::Inline {
                len: len as u8,
                data,
                p: PhantomData,
            })
        } else {
            None
        }
    }

    fn as_slice(&self) -> &[T] {
        match self {
            // special case for size 0 so we got proper alignment
            Self::Inline { len, .. } if *len == 0 => &[],
            Self::Inline { len, data, .. } => {
                mk_ref(slice_cast_raw::<u8, T>(&data[0], *len as usize).unwrap())
            }
            Self::ArcVecT { arc, .. } => arc.as_ref(),
            Self::ArcVecU8 { arc, .. } => slice_cast::<u8, T>(arc.as_ref()).unwrap(),
            Self::Custom { owner, extra, .. } => {
                slice_cast::<u8, T>(owner.get_slice(*extra)).unwrap()
            }
        }
    }
}

const fn mk_ref<'a, T>(pair: (*const T, usize)) -> &'a [T] {
    unsafe { std::mem::transmute(pair) }
}

impl<T> AsRef<[T]> for Blob<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Deref for Blob<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> Borrow<[T]> for Blob<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, sync::Arc};

    use super::Blob;
    use lazy_static::__Deref;
    use proptest::prelude::*;

    #[test]
    fn size() {
        #[cfg(target_pointer_width = "64")]
        assert_eq!(std::mem::size_of::<Blob<u8>>(), 32);
        #[cfg(target_pointer_width = "32")]
        assert_eq!(std::mem::size_of::<Blob<u8>>(), 16);
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
        fn inline(data in proptest::collection::vec(any::<u8>(), 0..24)) {
            if let Some(blob) = Blob::inline(&data) {
                prop_assert_eq!(&data, blob.as_ref());
                prop_assert!(data.len() <= 16);
            } else {
                prop_assert!(data.len() > 16);
            }
        }

        #[test]
        fn no_cast_typed(data in proptest::collection::vec(any::<u32>(), 0..24)) {
            let blob = Blob::arc_vec_t(Arc::new(data));
            prop_assert!(blob.cast::<u8>().is_err())
        }

        #[test]
        fn no_cast_inline(number in any::<u32>()) {
            let bytes = number.to_be_bytes();
            let blob = Blob::inline(&bytes).unwrap();
            prop_assert!(blob.cast::<u32>().is_err())
        }

        #[test]
        fn cast_arc_u8(bytes in any::<[u8; 8]>()) {
            let blob = Blob::<u8>::arc_u8(bytes.as_ref().into());
            let blob = blob.cast::<[u8; 8]>().unwrap();
            prop_assert!(blob.as_ref()[0] == bytes);
        }
    }
}
