use super::DecodeError;
use std::ops::Range;

/// Cursor over a payload in the peers' bincode-style encoding.
#[derive(Debug)]
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if len > self.remaining() {
            return Err(DecodeError::Truncated {
                offset: self.offset,
                needed: len - self.remaining(),
            });
        }
        let bytes = &self.bytes[self.offset..self.offset + len];
        self.offset += len;
        Ok(bytes)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    pub(crate) fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn bool(&mut self, field: &'static str) -> Result<bool, DecodeError> {
        let offset = self.offset;
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::Invalid { field, offset }),
        }
    }

    /// Reads a variable-length `u64`.
    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        let offset = self.offset;
        Ok(match self.byte()? {
            value @ 0..=250 => u64::from(value),
            251 => u64::from(u16::from_le_bytes(self.array()?)),
            252 => u64::from(u32::from_le_bytes(self.array()?)),
            253 => u64::from_le_bytes(self.array()?),
            _ => {
                return Err(DecodeError::Invalid {
                    field: "integer marker",
                    offset,
                });
            }
        })
    }

    /// Reads a variable-length `u32`, used for enum tags; the eight-byte form
    /// is rejected even when the value would fit.
    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        let offset = self.offset;
        Ok(match self.byte()? {
            value @ 0..=250 => u32::from(value),
            251 => u32::from(u16::from_le_bytes(self.array()?)),
            252 => u32::from_le_bytes(self.array()?),
            _ => {
                return Err(DecodeError::Invalid {
                    field: "tag marker",
                    offset,
                });
            }
        })
    }

    /// Reads a collection length whose items occupy at least `item_size`
    /// bytes each, rejecting counts the remaining payload cannot hold.
    pub(crate) fn count(&mut self, item_size: usize) -> Result<usize, DecodeError> {
        let offset = self.offset;
        let count = self.u64()?;
        let max = self.remaining() / item_size.max(1);
        match usize::try_from(count) {
            Ok(count) if count <= max => Ok(count),
            _ => Err(DecodeError::Invalid {
                field: "collection length",
                offset,
            }),
        }
    }

    /// Reads a length-prefixed byte string and returns its position.
    pub(crate) fn blob(&mut self) -> Result<Range<usize>, DecodeError> {
        let len = self.count(1)?;
        let start = self.offset;
        self.take(len)?;
        Ok(start..self.offset)
    }

    pub(crate) fn finish(self) -> Result<(), DecodeError> {
        match self.remaining() {
            0 => Ok(()),
            remaining => Err(DecodeError::Trailing { remaining }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_varint_boundaries() {
        let bytes = [
            250, 251, 251, 0, 252, 0, 0, 1, 0, 253, 0, 0, 0, 0, 1, 0, 0, 0,
        ];
        let mut reader = Reader::new(&bytes);
        for expected in [250, 251, 65_536, 4_294_967_296] {
            assert_eq!(reader.u64().unwrap(), expected);
        }
        reader.finish().unwrap();
    }

    #[test]
    fn tags_reject_the_eight_byte_form() {
        assert!(Reader::new(&[253, 1, 0, 0, 0, 0, 0, 0, 0]).u32().is_err());
        assert_eq!(Reader::new(&[251, 3, 0]).u32().unwrap(), 3);
    }

    #[test]
    fn counts_are_bounded_by_the_remaining_bytes() {
        assert!(Reader::new(&[2, 0xaa]).count(1).is_err());
        assert_eq!(Reader::new(&[1, 0xaa]).count(1).unwrap(), 1);
        let huge = [253, 255, 255, 255, 255, 255, 255, 255, 255];
        assert!(Reader::new(&huge).count(32).is_err());
    }

    #[test]
    fn reports_truncation_and_trailing_bytes() {
        let mut reader = Reader::new(&[251, 1]);
        assert_eq!(
            reader.u64(),
            Err(DecodeError::Truncated {
                offset: 1,
                needed: 1
            })
        );
        assert_eq!(
            Reader::new(&[1]).finish(),
            Err(DecodeError::Trailing { remaining: 1 })
        );
    }
}
