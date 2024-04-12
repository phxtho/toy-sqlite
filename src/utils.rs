use std::io::Read;

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
