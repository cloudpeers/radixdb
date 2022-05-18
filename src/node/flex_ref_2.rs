use std::{marker::PhantomData, sync::Arc};

struct FlexRef<T>(u8, PhantomData<T>, [u8]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Type {
    None,
    Inline,
    Id,
    Arc,
}

#[repr(transparent)]
struct TreePrefix(FlexRef<Vec<u8>>);

impl TreePrefix {
    fn read<'a>(value: &'a [u8]) -> anyhow::Result<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read(value)?;
        // todo: use ref_cast
        Ok((unsafe { std::mem::transmute(f)}, rest))
    }
}

#[repr(transparent)]
struct TreeValue(FlexRef<Vec<u8>>);

impl TreeValue {
    fn read<'a>(value: &'a [u8]) -> anyhow::Result<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read(value)?;
        // todo: use ref_cast
        Ok((unsafe { std::mem::transmute(f)}, rest))
    }
}

#[repr(transparent)]
struct TreeChildren(FlexRef<Vec<u8>>);

impl TreeChildren {
    fn read<'a>(value: &'a [u8]) -> anyhow::Result<(&'a Self, &'a [u8])> {
        let (f, rest): (&'a FlexRef<Vec<u8>>, &'a [u8]) = FlexRef::read(value)?;
        // todo: use ref_cast
        Ok((unsafe { std::mem::transmute(f)}, rest))
    }

    fn iter(&self) -> TreeChildrenIterator<'_> {
        TreeChildrenIterator(self.0.as_ref())
    }
}

#[repr(transparent)]
struct TreeChildrenIterator<'a>(&'a [u8]);

impl<'a> Iterator for TreeChildrenIterator<'a> {
    type Item = anyhow::Result<TreeNode<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            Some(match TreeNode::read(&mut self.0) {
                Ok((res, rest)) => {
                    self.0 = rest;
                    Ok(res)
                }
                Err(e) => {
                    Err(e)
                }
            })
        }
    }
}

struct OwnedTreeNode(smallvec::SmallVec<[u8; 16]>);

struct TreeNode<'a> {
    prefix: &'a TreePrefix,
    value: &'a TreeValue,
    children: &'a TreeChildren,
}

impl<'a> TreeNode<'a> {

    fn read(buffer: &'a [u8]) -> anyhow::Result<(Self, &'a [u8])> {
        let (prefix, buffer) = TreePrefix::read(buffer)?;
        let (value, buffer) = TreeValue::read(buffer)?;
        let (children, buffer) = TreeChildren::read(buffer)?;
        Ok((Self {
            prefix, value, children,
        }, buffer))
    }
}

impl<T> FlexRef<T> {

    fn read(value: &[u8]) -> anyhow::Result<(&Self, &[u8])> {
        anyhow::ensure!(value.len() > 0);
        let len = len(value[0]);
        anyhow::ensure!(len <= value.len());
        let x: &Self = unsafe { std::mem::transmute(value) };
        Ok((x, &value[len..]))
    }

    fn tpe(&self) -> Type {
        tpe(self.0)
    }

    fn len(&self) -> usize {
        len(self.0) + 1
    }

    fn inline_as_ref(&self) -> Option<&[u8]> {
        if self.tpe() == Type::Inline {
            let len = len(self.0);
            Some(&self.2[0..len])
        } else {
            None
        }
    }
    
    fn arc_as_ref(&self) -> Option<&Arc<T>> {
        if self.tpe() == Type::Arc {
            let arc: &Arc<T> = unsafe { std::mem::transmute(&self.0) };
            Some(arc)
        } else {
            None
        }
    }

    fn is_none(&self) -> bool {
        self.tpe() == Type::None
    }

    fn is_some(&self) -> bool {
        !self.is_some()
    }
}

fn len(value: u8) -> usize {
    match tpe(value) {
        Type::None => 0,
        Type::Id => 8,
        Type::Arc => 8,
        Type::Inline => ((value & 0x7f) as usize),
    }
}

fn tpe(value: u8) -> Type {
    if value == 0 {
        Type::None
    } else if value & 0x80 != 0 {
        Type::Inline
    } else if value & 0x40 == 0 {
        Type::Id
    } else {
        Type::Arc
    }
}

impl AsRef<[u8]> for FlexRef<Vec<u8>> {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}