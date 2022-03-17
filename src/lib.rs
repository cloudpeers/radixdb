use std::sync::Arc;

mod blob;
mod blob_store;
mod flex_ref;
mod iterators;
mod merge_state;
#[cfg(not(target_arch = "wasm32"))]
mod paged_file_store;
#[cfg(not(target_arch = "wasm32"))]
mod paged_file_store2;
mod tree;
pub(crate) use blob::Blob;
pub use blob_store::{DynBlobStore, MemStore, NoStore, PagedMemStore, NO_STORE};

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
const NONE_ARRAY_U64: u64 = u64::from_be_bytes(NONE_ARRAY);

const DISC_INLINE: u8 = 0;
const DISC_ID_EXTRA: u8 = 1;
const DISC_ID_NONE: u8 = 2;
const DISC_NONE: u8 = 0x3f;

/// get the type of a special value. It is bit 3..8 of the first byte
const fn type_discriminator(bytes: [u8; 8]) -> u8 {
    bytes[0] >> 2
}

/// get the extra byte of a special value.
const fn extra_byte(bytes: [u8; 8]) -> u8 {
    ((bytes[0] & 2) << 6) | (bytes[7] >> 1)
}

/// make a special value with discriminator and extra
const fn mk_bytes(discriminator: u8, extra: u8) -> [u8; 8] {
    let b0 = (discriminator << 2) | ((extra & 0x80) >> 6) | 1;
    let b7 = (extra << 1) | 1;
    [b0, 0, 0, 0, 0, 0, 0, b7]
}

const fn from_native_bytes(bytes: [u8; 8]) -> u64 {
    unsafe { std::mem::transmute(bytes) }
}

const fn is_pointer(bytes: [u8; 8]) -> bool {
    (from_native_bytes(bytes) & SPECIAL_MASK_U64) == 0
}

const fn is_extra(bytes: [u8; 8]) -> bool {
    (from_native_bytes(bytes) & SPECIAL_MASK_U64) == SPECIAL_MASK_U64
}

const fn is_none(bytes: [u8; 8]) -> bool {
    from_native_bytes(bytes) == NONE_ARRAY_U64
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

const fn aligned_empty_ref() -> &'static [u8] {
    let t: &'static [u128] = &[];
    unsafe { std::mem::transmute::<&[u128], &[u8]>(t) }
}

fn arc_ref_mut<T>(value: &mut [u8; 8]) -> &mut Arc<T> {
    // todo: pretty sure this is broken on 32 bit!
    unsafe { std::mem::transmute(value) }
}

/// extract a pointer from 8 bytes
const fn from_ptr(value: usize) -> [u8; 8] {
    let value: u64 = value as u64;
    assert!((value & 1) == 0 && (value & 0x0100_0000_0000_0000u64 == 0));
    unsafe { std::mem::transmute(value) }
}

const fn from_arc<T>(arc: Arc<T>) -> [u8; 8] {
    from_ptr(unsafe { std::mem::transmute(arc) })
}

/// Utility to output something as hex
struct Hex<'a>(&'a [u8], usize);

impl<'a> Hex<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self(data, data.len())
    }
    fn partial(data: &'a [u8], len: usize) -> Self {
        let display = if len < data.len() { &data[..len] } else { data };
        Self(display, data.len())
    }
}

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() < self.1 {
            write!(f, "[{}..., {} bytes]", hex::encode(self.0), self.1)
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", hex::encode(self.0))
    }
}

fn slice_cast<T, U>(value: &[T]) -> anyhow::Result<&[U]> {
    let (ptr, size) = unsafe { std::mem::transmute(value) };
    let (ptr, size) = slice_cast_raw::<T, U>(ptr, size)?;
    Ok(unsafe { std::mem::transmute((ptr, size)) })
}

fn slice_cast_raw<T, U>(ptr: *const T, tsize: usize) -> anyhow::Result<(*const U, usize)> {
    let bytes = tsize * std::mem::size_of::<T>();
    anyhow::ensure!(
        (ptr as usize) % std::mem::align_of::<U>() == 0,
        "pointer is not properly aligned for target type. std.mem::align_of::<{}>() is {}",
        std::any::type_name::<T>(),
        std::mem::align_of::<U>(),
    );
    anyhow::ensure!(
        bytes % std::mem::size_of::<U>() == 0,
        "byte size is not a multiple of target size. std::mem::size_of::<{}>() is {}",
        std::any::type_name::<T>(),
        std::mem::size_of::<U>(),
    );
    let usize = bytes / std::mem::size_of::<U>();
    let ptr = ptr as *const U;
    Ok((ptr, usize))
}

// common prefix of two slices.
fn common_prefix<'a, T: Eq>(a: &'a [T], b: &'a [T]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

fn read_length_prefixed(source: &[u8], offset: usize) -> &[u8] {
    let len = u32::from_be_bytes(source[offset..offset + 4].try_into().unwrap()) as usize;
    &source[offset + 4..offset + 4 + len]
}

fn write_length_prefixed(target: &mut [u8], offset: usize, slice: &[u8]) {
    let len_u32: u32 = slice.len().try_into().unwrap();
    target[offset..offset + 4].copy_from_slice(&len_u32.to_be_bytes());
    target[offset + 4..offset + 4 + slice.len()].copy_from_slice(slice);
}

#[test]
fn discriminator_and_extra() {
    for extra in 0..255u8 {
        for discriminator in 0..63u8 {
            let bytes = mk_bytes(discriminator, extra);
            assert!(is_extra(bytes));
            assert_eq!(bytes[1..7], [0, 0, 0, 0, 0, 0u8]);
            assert_eq!(type_discriminator(bytes), discriminator);
            assert_eq!(extra_byte(bytes), extra);
        }
    }
}
