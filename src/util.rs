pub struct Lit(pub String);

impl std::fmt::Debug for Lit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> std::fmt::Display for Lit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Utility to output something as hex
pub struct Hex<'a>(&'a [u8], usize);

impl<'a> Hex<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data, data.len())
    }

    #[allow(dead_code)]
    pub fn partial(data: &'a [u8], len: usize) -> Self {
        Self(data, len)
    }
}

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() > self.1 {
            write!(
                f,
                "[{}..., {} bytes]",
                hex::encode(&self.0[..self.1]),
                self.0.len()
            )
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() > self.1 {
            write!(
                f,
                "[{}..., {} bytes]",
                hex::encode(&self.0[..self.1]),
                self.0.len()
            )
        } else {
            write!(f, "[{}]", hex::encode(self.0))
        }
    }
}
