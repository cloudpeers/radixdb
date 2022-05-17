use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::Hex;

/// A compact representation of an inline or heap allocated object or an id.
///
/// Guaranteed to be 64 bit on every platform.
#[repr(transparent)]
#[derive(PartialEq, Eq)]
pub struct FlexRef<T>(u64, PhantomData<T>);

impl<T> Debug for FlexRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.id_value() {
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
    #[inline(always)]
    const fn new(value: u64) -> Self {
        Self(value, PhantomData)
    }
    #[inline(always)]
    const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self::new(unsafe { std::mem::transmute(bytes) })
    }

    pub const fn bytes(&self) -> &[u8; 8] {
        unsafe { std::mem::transmute(&self.0) }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8; 8] {
        unsafe { std::mem::transmute(&mut self.0) }
    }

    /// the none marker value
    pub const fn none() -> Self {
        Self::new(NONE_ARRAY_U64)
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
            Some(Self::from_bytes(bytes))
        } else {
            None
        }
    }

    pub const INLINE_EMPTY_ARRAY: Self = Self::from_bytes(mk_bytes(DISC_INLINE, 0));

    /// try to create an inline value from a slice, if it fits
    pub fn inline_from_slice(value: &[u8]) -> Option<Self> {
        let len = value.len();
        if len < 7 {
            let mut res = mk_bytes(DISC_INLINE, len as u8);
            res[1..=len].copy_from_slice(value);
            Some(Self::from_bytes(res))
        } else {
            None
        }
    }

    /// create an owned from an arc to a sized thing
    pub const fn owned_from_arc(arc: Arc<T>) -> Self {
        let addr: usize = unsafe { std::mem::transmute(arc) };
        Self::new(from_ptr(addr))
    }

    pub const fn inline_len(&self) -> Option<usize> {
        if self.is_inline() {
            Some(extra_byte(self.0) as usize)
        } else {
            None
        }
    }

    pub fn inline_as_ref(&self) -> Option<&[u8]> {
        if self.is_inline() {
            let len = extra_byte(self.0) as usize;
            if len == 0 {
                // special case so the empty ref is aligned
                Some(ALIGNED_EMPTY_REF)
            } else {
                Some(&self.bytes()[1..=len])
            }
        } else {
            None
        }
    }

    pub const fn id_value(&self) -> Option<u64> {
        if self.is_id() {
            let mut res = [0u8; 8];
            // this is so the fn can be const
            let b = self.bytes();
            res[2] = b[1];
            res[3] = b[2];
            res[4] = b[3];
            res[5] = b[4];
            res[6] = b[5];
            res[7] = b[6];
            Some(u64::from_be_bytes(res))
        } else {
            None
        }
    }

    pub const fn id_extra(&self) -> Option<Option<u8>> {
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
            self.0 = NONE_ARRAY_U64;
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

    pub const fn owned_arc_ref(&self) -> Option<&Arc<T>> {
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
    /// gets the optional first byte
    pub fn first_byte_opt(&self) -> Option<u8> {
        if let Some(len) = self.inline_len() {
            if len > 0 {
                Some(self.bytes()[1])
            } else {
                None
            }
        } else if let Some(arc) = self.owned_arc_ref() {
            arc.get(0).cloned()
        } else if let Some(extra) = self.id_extra() {
            extra
        } else {
            None
        }
    }

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
            Self::new(self.0)
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

/// Mask for "special" values. No pointer will ever have these bits set at the same time.
///
/// On a 32 bit system, either the lower or the higher half will be 0 depending on endianness
/// On a 64 bit system, the upper 2 bytes are typically not used, and pointers are aligned
///
/// 32 bit, be [x, x, x, x, 0, 0, 0, 0]
/// 32 bit, le [0, 0, 0, 0, x, x, x, x]
/// 64 bit, be [0, 0, x, x, x, x, x, e]
/// 64 bit, le [e, x, x, x, x, x, 0, 0]
const SPECIAL_MASK: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 1];
const SPECIAL_MASK_U64: u64 = u64::from_be_bytes(SPECIAL_MASK);
const NONE_ARRAY: [u8; 8] = [255, 255, 255, 255, 255, 255, 255, 255];
const NONE_ARRAY_U64: u64 = u64::from_be_bytes(NONE_ARRAY);

const DISC_INLINE: u8 = 0;
const DISC_ID_EXTRA: u8 = 1;
const DISC_ID_NONE: u8 = 2;
const DISC_NONE: u8 = 0x3f;

/// get the type of a special value. It is bit 3..8 of the first byte
#[inline]
const fn type_discriminator(bytes: u64) -> u8 {
    (bytes as u8) >> 2
}

/// get the extra byte of a special value.
#[inline]
const fn extra_byte(bytes: u64) -> u8 {
    (((bytes as u8) & 2) << 6) | ((bytes >> 57) as u8)
}

/// make a special value with discriminator and extra
#[inline]
const fn mk_bytes(discriminator: u8, extra: u8) -> [u8; 8] {
    let b0 = (discriminator << 2) | ((extra & 0x80) >> 6) | 1;
    let b7 = (extra << 1) | 1;
    [b0, 0, 0, 0, 0, 0, 0, b7]
}

#[inline]
const fn from_native_bytes(bytes: [u8; 8]) -> u64 {
    unsafe { std::mem::transmute(bytes) }
}

#[inline]
const fn is_pointer(bytes: u64) -> bool {
    (bytes & SPECIAL_MASK_U64) == 0
}

#[inline]
const fn is_extra(bytes: u64) -> bool {
    (bytes & SPECIAL_MASK_U64) == SPECIAL_MASK_U64
}

#[inline]
const fn is_none(bytes: u64) -> bool {
    bytes == NONE_ARRAY_U64
}

#[inline]
/// extract a pointer from 8 bytes
const fn ptr(value: u64) -> usize {
    debug_assert!(
        value <= (usize::MAX as u64),
        "got 64 bit pointer on a 32 bit system"
    );
    value as usize
}

#[inline]
const fn arc<T>(value: u64) -> Arc<T> {
    unsafe { std::mem::transmute(ptr(value)) }
}

#[inline]
const fn arc_ref<T>(value: &u64) -> &Arc<T> {
    // todo: pretty sure this is broken on 32 bit!
    unsafe { std::mem::transmute(value) }
}

const ALIGNED_EMPTY_REF: &'static [u8] = {
    let t: &'static [u128] = &[];
    unsafe { std::mem::transmute::<&[u128], &[u8]>(t) }
};

#[inline]
fn arc_ref_mut<T>(value: &mut u64) -> &mut Arc<T> {
    // todo: pretty sure this is broken on 32 bit!
    unsafe { std::mem::transmute(value) }
}

#[inline]
/// extract a pointer from 8 bytes
const fn from_ptr(value: usize) -> u64 {
    let value: u64 = value as u64;
    assert!((value & 1) == 0 && (value & 0x0100_0000_0000_0000u64 == 0));
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use proptest::prelude::*;

    #[test]
    fn miri_endianness() {
        let x = FlexRef::<u64>::id_from_u64_and_extra(12345678, Some(9)).unwrap();
        println!("{}", std::mem::size_of::<usize>());
        println!("{}", Hex::new(x.bytes()));
        assert_eq!(x.bytes(), &[5, 0, 0, 0, 188, 97, 78, 19]);

        let x = FlexRef::<u64>::inline_from_slice(&[1, 2, 3, 4, 5]).unwrap();
        println!("{}", std::mem::size_of::<usize>());
        println!("{}", Hex::new(x.bytes()));
        assert_eq!(x.bytes(), &[1, 1, 2, 3, 4, 5, 0, 11]);

        let x = FlexRef::<u64>::INLINE_EMPTY_ARRAY;
        println!("{}", std::mem::size_of::<usize>());
        println!("{}", Hex::new(x.bytes()));
        assert_eq!(x.bytes(), &[1, 0, 0, 0, 0, 0, 0, 1]);

        let x = FlexRef::<u64>::none();
        println!("{}", std::mem::size_of::<usize>());
        println!("{}", Hex::new(x.bytes()));
        assert_eq!(x.bytes(), &[255, 255, 255, 255, 255, 255, 255, 255]);
    }

    proptest! {

        #[test]
        fn flexref_id_and_extra_roundtrip(id in 0u64..=0x0000_FFFF__FFFF_FFFFu64, extra in any::<Option<u8>>()) {
            let f = FlexRef::<u8>::id_from_u64_and_extra(id, extra).unwrap();
            info!("{} {:?}", id, f);
            prop_assert_eq!(Some(id), f.id_value());
            prop_assert_eq!(Some(extra), f.id_extra());
        }

        #[test]
        fn flexref_inline_roundtrip(inline in proptest::collection::vec(any::<u8>(), 0..6)) {
            let f = FlexRef::<u8>::inline_from_slice(&inline).unwrap();
            info!("{:x?} {:?}", inline, f);
            prop_assert_eq!(Some(inline.as_slice()), f.inline_as_ref());
        }
    }

    #[test]
    fn discriminator_and_extra() {
        for extra in 0..255u8 {
            for discriminator in 0..63u8 {
                let bytes = mk_bytes(discriminator, extra);
                assert!(is_extra(unsafe { std::mem::transmute(bytes) }));
                assert_eq!(bytes[1..7], [0, 0, 0, 0, 0, 0u8]);
                assert_eq!(
                    type_discriminator(unsafe { std::mem::transmute(bytes) }),
                    discriminator
                );
                assert_eq!(extra_byte(unsafe { std::mem::transmute(bytes) }), extra);
            }
        }
    }
}
