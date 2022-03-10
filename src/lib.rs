use std::sync::Arc;

mod owned;
mod merge_state;
mod iterators;

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
const ID_MASK: [u8; 8] = [253, 0, 0, 0, 0, 0, 0, 253];
const EMPTY_INLINE_ARRAY: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 1];
const NONE_ARRAY: [u8; 8] = [255, 255, 255, 255, 255, 255, 255, 255];

const fn from_native_bytes(bytes: [u8; 8]) -> u64 {
    unsafe { std::mem::transmute(bytes) }
}

const fn is_pointer(bytes: [u8; 8]) -> bool {
    (from_native_bytes(bytes) & SPECIAL_MASK_U64) == 0
}

/// extract a pointer from 8 bytes
const fn ptr(value: [u8; 8]) -> usize {
    let value: u64 = from_native_bytes(value);
    assert!(
        value <= (usize::MAX as u64),
        "got 64 bit pointer on a 32 bit system"
    );
    value as usize
}

const fn arc<T>(value: [u8; 8]) -> Arc<T> {
    unsafe { std::mem::transmute(ptr(value)) }
}

const fn arc_ref<T>(value: &[u8; 8]) -> &Arc<T> {
    // todo: pretty sure this is broken on 32 bit!
    unsafe { std::mem::transmute(value) }
}

/// extract a pointer from 8 bytes
fn from_ptr(value: usize) -> [u8; 8] {
    let value: u64 = value.try_into().expect("usize < 64 bit");
    assert!((value & 1) == 0 && (value & 0x0100_0000_0000_0000u64 == 0));
    unsafe { std::mem::transmute(value) }
}

fn from_arc<T>(arc: Arc<T>) -> [u8; 8] {
    from_ptr(unsafe { std::mem::transmute(arc) })
}

/// Utility to output something as hex
struct Hex<'a>(&'a [u8]);

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", hex::encode(self.0))
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", hex::encode(self.0))
    }
}

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}