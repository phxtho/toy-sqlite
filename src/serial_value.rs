use std::{cmp::min, io::Read};

use crate::serial_type::SerialType;

#[derive(PartialEq, Debug, Clone)]
pub enum SerialValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub fn parse_value<T: Read>(reader: &mut T, serial_type: SerialType) -> SerialValue {
    match serial_type {
        SerialType::Int8
        | SerialType::Int16
        | SerialType::Int24
        | SerialType::Int32
        | SerialType::Int48
        | SerialType::Int64 => {
            const MAX_SIZE: usize = 8;
            let buffer_size = min(SerialType::size(serial_type), MAX_SIZE);
            let mut buf = vec![0u8; buffer_size];
            let _ = reader.read_exact(&mut buf);
            let mut byte_array = [0u8; MAX_SIZE];
            byte_array[MAX_SIZE - buffer_size..MAX_SIZE].copy_from_slice(&buf[..buffer_size]);
            SerialValue::Int(i64::from_be_bytes(byte_array))
        }
        SerialType::Zero => SerialValue::Int(0),
        SerialType::One => SerialValue::Int(1),
        SerialType::Float64 => {
            let mut buf = [0u8; 8];
            let _ = reader.read_exact(&mut buf);
            SerialValue::Float(f64::from_be_bytes(buf))
        }
        SerialType::Blob(size) => {
            let mut buf = vec![0u8; size];
            let _ = reader.read_exact(&mut buf);
            SerialValue::Blob(buf)
        }
        SerialType::Text(size) => {
            let mut buf = vec![0u8; size];
            let _ = reader.read_exact(&mut buf);
            let out_str = String::from_utf8(buf).expect("failed to parse Text to string");
            SerialValue::Text(out_str)
        }
        SerialType::Null => SerialValue::Null,
    }
}

#[cfg(test)]
mod parse_values_tests {
    use std::io::Cursor;

    use crate::serial_type::SerialType;

    use super::*;
    #[test]
    fn test_parse_value_int8() {
        let mut reader = Cursor::new(vec![0x01]);
        let value = parse_value(&mut reader, SerialType::Int8);
        assert_eq!(value, SerialValue::Int(1));
    }
    #[test]
    fn test_parse_value_int64() {
        let mut reader = Cursor::new(vec![127, 255, 255, 255, 255, 255, 255, 255]);
        let value = parse_value(&mut reader, SerialType::Int64);
        assert_eq!(value, SerialValue::Int(9223372036854775807));
    }

    #[test]
    fn test_parse_value_float() {
        let mut reader = Cursor::new(vec![0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let value = parse_value(&mut reader, SerialType::Float64);
        assert_eq!(value, SerialValue::Float(12.5));
    }

    #[test]
    fn test_parse_value_blob() {
        let mut reader = Cursor::new(vec![0x01, 0x02, 0x03, 0x04]);
        let value = parse_value(&mut reader, SerialType::Blob(4));
        assert_eq!(value, SerialValue::Blob(vec![0x01, 0x02, 0x03, 0x04]));
    }

    #[test]
    fn test_parse_value_text() {
        let mut reader = Cursor::new(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]);
        let value = parse_value(&mut reader, SerialType::Text(5));
        assert_eq!(value, SerialValue::Text("Hello".to_string()));
    }

    #[test]
    fn test_parse_value_null() {
        let mut reader = Cursor::new(vec![]);
        let value = parse_value(&mut reader, SerialType::Null);
        assert_eq!(value, SerialValue::Null);
    }
}
