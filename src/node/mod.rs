mod flex_ref;
mod merge_state;
mod owned_slice;
mod tree;
pub use flex_ref::FlexRef;
pub use merge_state::NodeConverter;
pub use owned_slice::OwnedSlice;
pub use tree::{Tree, TreeChildren, TreeNode, TreePrefix, TreeValue};

fn slice_cast<T, U>(value: &[T]) -> anyhow::Result<&[U]> {
    let (ptr, size) = unsafe { std::mem::transmute(value) };
    let (ptr, size) = slice_cast_raw::<T, U>(ptr, size)?;
    Ok(unsafe { std::mem::transmute((ptr, size)) })
}

fn slice_cast_raw<T, U>(ptr: *const T, tsize: usize) -> anyhow::Result<(*const U, usize)> {
    let bytes = tsize * std::mem::size_of::<T>();
    let align = (ptr as usize) % std::mem::align_of::<U>();
    anyhow::ensure!(
        align == 0,
        "pointer is not properly aligned for target type. std.mem::align_of::<{}>() is {}, align is {}",
        std::any::type_name::<T>(),
        std::mem::align_of::<U>(),
        align,
    );
    anyhow::ensure!(
        bytes % std::mem::size_of::<U>() == 0,
        "byte size is not a multiple of target size. std::mem::size_of::<{}>() is {}, byte length is {}",
        std::any::type_name::<T>(),
        std::mem::size_of::<U>(),
        bytes,
    );
    let usize = bytes / std::mem::size_of::<U>();
    let ptr = ptr as *const U;
    Ok((ptr, usize))
}
