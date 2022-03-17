use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use super::*;

#[repr(C, align(8))]
#[derive(PartialEq, Eq)]
pub(crate) struct FlexRef<T>(pub [u8; 8], PhantomData<T>);

impl<T> Debug for FlexRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.id_u64() {
            f.debug_tuple("FlexRef::Id").field(&id).finish()
        } else if let Some(data) = self.inline_as_ref() {
            f.debug_tuple("FlexRef::Inline")
                .field(&Hex::new(data))
                .finish()
        } else if let Some(arc) = self.owned_arc_ref() {
            f.debug_struct("FlexRef::Owned")
                .field("p", &Arc::as_ptr(arc))
                .field("c", &Arc::strong_count(arc))
                .finish()
        } else {
            f.debug_tuple("FlexRef::None").finish()
        }
    }
}

impl<T> FlexRef<T> {
    /// the none marker value
    pub const fn none() -> Self {
        Self(NONE_ARRAY, PhantomData)
    }

    /// try to create an id from an u64, if it fits
    pub const fn id_from_u64_and_extra(value: u64, extra: Option<u8>) -> Option<Self> {
        let id_bytes = value.to_be_bytes();
        if id_bytes[0] == 0 || id_bytes[1] == 0 {
            let mut bytes = if let Some(extra) = extra {
                mk_bytes(DISC_ID_EXTRA, extra)
            } else {
                mk_bytes(DISC_ID_NONE, 0)
            };
            // this is so the fn can be const
            bytes[1] = id_bytes[2];
            bytes[2] = id_bytes[3];
            bytes[3] = id_bytes[4];
            bytes[4] = id_bytes[5];
            bytes[5] = id_bytes[6];
            bytes[6] = id_bytes[7];
            Some(Self(bytes, PhantomData))
        } else {
            None
        }
    }

    /// try to create an id from an u64, if it fits
    ///
    /// will use None as extra data
    pub const fn id_from_u64(value: u64) -> Option<Self> {
        Self::id_from_u64_and_extra(value, None)
    }

    /// the inline empty array
    pub const fn inline_empty_array() -> Self {
        Self(mk_bytes(DISC_INLINE, 0), PhantomData)
    }

    /// try to create an inline value from a slice, if it fits
    pub fn inline_from_slice(value: &[u8]) -> Option<Self> {
        let len = value.len();
        if len < 7 {
            let mut res = mk_bytes(DISC_INLINE, len as u8);
            res[1..=len].copy_from_slice(value);
            Some(Self(res, PhantomData))
        } else {
            None
        }
    }

    /// create an owned from an arc to a sized thing
    pub const fn owned_from_arc(arc: Arc<T>) -> Self {
        let addr: usize = unsafe { std::mem::transmute(arc) };
        Self(from_ptr(addr), PhantomData)
    }

    pub fn inline_as_ref(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = extra_byte(self.0) as usize;
            if len == 0 {
                // special case so the empty ref is aligned
                Some(aligned_empty_ref())
            } else {
                Some(&self.0[1..=len])
            }
        } else {
            None
        }
    }

    pub const fn id_u64(&self) -> Option<u64> {
        if self.is_id() {
            let mut res = [0u8; 8];
            // this is so the fn can be const
            res[2] = self.0[1];
            res[3] = self.0[2];
            res[4] = self.0[3];
            res[5] = self.0[4];
            res[6] = self.0[5];
            res[7] = self.0[6];
            Some(u64::from_be_bytes(res))
        } else {
            None
        }
    }

    pub const fn id_extra_data(&self) -> Option<Option<u8>> {
        if !is_extra(self.0) {
            None
        } else if type_discriminator(self.0) == DISC_ID_EXTRA {
            Some(Some(extra_byte(self.0)))
        } else if type_discriminator(self.0) == DISC_ID_NONE {
            Some(None)
        } else {
            None
        }
    }

    pub const fn is_arc(&self) -> bool {
        is_pointer(self.0)
    }

    pub const fn is_copy(&self) -> bool {
        // for now, the only thing that is not copy is an arc
        is_extra(self.0)
    }

    pub const fn is_inline(&self) -> bool {
        is_extra(self.0) && (type_discriminator(self.0) == DISC_INLINE)
    }

    pub const fn is_id(&self) -> bool {
        is_extra(self.0) && {
            let tpe = type_discriminator(self.0);
            tpe == DISC_ID_NONE || tpe == DISC_ID_EXTRA
        }
    }

    pub const fn is_none(&self) -> bool {
        is_none(self.0)
    }

    pub fn owned_take_arc(&mut self) -> Option<Arc<T>> {
        if is_pointer(self.0) {
            let res = arc(self.0);
            self.0 = NONE_ARRAY;
            Some(res)
        } else {
            None
        }
    }

    pub fn owned_into_arc(self) -> Option<Arc<T>> {
        if is_pointer(self.0) {
            let res = arc(self.0);
            std::mem::forget(self);
            Some(res)
        } else {
            None
        }
    }

    pub fn owned_arc_ref(&self) -> Option<&Arc<T>> {
        if is_pointer(self.0) {
            Some(arc_ref(&self.0))
        } else {
            None
        }
    }

    pub fn owned_arc_ref_mut(&mut self) -> Option<&mut Arc<T>> {
        if is_pointer(self.0) {
            Some(arc_ref_mut(&mut self.0))
        } else {
            None
        }
    }
}

impl FlexRef<Vec<u8>> {
    pub fn inline_or_owned_from_slice(value: &[u8]) -> Self {
        if let Some(res) = FlexRef::inline_from_slice(value) {
            res
        } else {
            FlexRef::owned_from_arc(Arc::new(value.to_vec()))
        }
    }
    pub fn inline_or_owned_from_vec(value: Vec<u8>) -> Self {
        if let Some(res) = FlexRef::inline_from_slice(&value) {
            res
        } else {
            FlexRef::owned_from_arc(Arc::new(value))
        }
    }
}

impl<T> Clone for FlexRef<T> {
    fn clone(&self) -> Self {
        if let Some(arc) = self.owned_arc_ref() {
            Self::owned_from_arc(arc.clone())
        } else {
            Self(self.0, PhantomData)
        }
    }
}

impl<T> Drop for FlexRef<T> {
    fn drop(&mut self) {
        if let Some(arc) = self.owned_take_arc() {
            drop(arc)
        }
    }
}
