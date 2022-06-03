pub mod node;
pub mod store;
pub use node::VSTree as VSRadixTree;

#[cfg(test)]
#[macro_use]
extern crate maplit;

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
