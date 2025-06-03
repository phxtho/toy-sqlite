use std::io::Read;

/// read a variable length integer returning a tuple of the value and number of bytes read
pub fn read_varint<T: Read>(reader: &mut T) -> (u64, u64) {
    let mut read_more = true;
    let mut result: u64 = 0;
    let mut bytes_read = 0;

    while read_more {
        // read first byte
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).expect("failed to read varint");
        bytes_read += 1;

        // check high bit for continuation
        let high_bit = buf[0] & 0b1000_0000;
        read_more = high_bit != 0;

        // make space for next 7 bits
        result <<= 7;
        // isolate low 7 bits and add to result
        let low_bits = buf[0] & 0b0111_1111;
        result |= low_bits as u64;
    }

    return (result, bytes_read);
}

#[test]
fn test_read_varint() {
    let mut buf = std::io::Cursor::new(vec![0b0000_0001, 0b0000_0001]);
    assert_eq!(read_varint(&mut buf), (1, 1));
    assert_eq!(read_varint(&mut buf), (1, 1));
}

#[test]
fn test_read_varint_reading_zero() {
    let mut buf = std::io::Cursor::new(vec![0b0000]);
    assert_eq!(read_varint(&mut buf), (0, 1));
}

#[test]
fn test_read_varint_reading_continuation_bits() {
    let mut buf = std::io::Cursor::new(vec![0b10000111, 0b01101000]);
    assert_eq!(read_varint(&mut buf), (1000, 2));
}

#[test]
fn test_read_varint_large_value() {
    // should be 2^63-1 because one of the 64 bits should be continuation
    let largest_number: u64 = (2 as u64).pow(63) - 1;
    let mut buf = std::io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]); // 11111111,..., 01111111
    assert_eq!(read_varint(&mut buf), (largest_number, 9));
}
