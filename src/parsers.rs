use std::{cmp::min, io::Read};

use anyhow::{bail, Error};

use crate::{
    structs::{BTreeHeader, BTreePageType, ColumnHeader, Record, RecordHeader, SerialType, Value},
    utils::read_varint,
};

fn parse_value<T: Read>(reader: &mut T, serial_type: SerialType) -> Value {
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
            let mut array = [0u8; MAX_SIZE];
            array[MAX_SIZE - buffer_size..MAX_SIZE].copy_from_slice(&buf[..buffer_size]);
            Value::Int(i64::from_be_bytes(array))
        }
        SerialType::Zero => Value::Int(0),
        SerialType::One => Value::Int(1),
        SerialType::Float64 => {
            let mut buf = [0u8; 8];
            let _ = reader.read_exact(&mut buf);
            Value::Float(f64::from_be_bytes(buf))
        }
        SerialType::Blob(size) => {
            let mut buf = vec![0u8; size];
            let _ = reader.read_exact(&mut buf);
            Value::Blob(buf)
        }
        SerialType::Text(size) => {
            let mut buf = vec![0u8; size];
            let _ = reader.read_exact(&mut buf);
            let out_str = String::from_utf8(buf).expect("failed to parse Text to string");
            Value::Text(out_str)
        }
        SerialType::Null => Value::Null,
    }
}

#[cfg(test)]
mod parser_tests {
    use std::io::Cursor;

    use super::*;
    #[test]
    fn test_parse_value_int8() {
        let mut reader = Cursor::new(vec![0x01]);
        let value = parse_value(&mut reader, SerialType::Int8);
        assert_eq!(value, Value::Int(1));
    }
    #[test]
    fn test_parse_value_int64() {
        let mut reader = Cursor::new(vec![127, 255, 255, 255, 255, 255, 255, 255]);
        let value = parse_value(&mut reader, SerialType::Int64);
        assert_eq!(value, Value::Int(9223372036854775807));
    }

    #[test]
    fn test_parse_value_float() {
        let mut reader = Cursor::new(vec![0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let value = parse_value(&mut reader, SerialType::Float64);
        assert_eq!(value, Value::Float(12.5));
    }

    #[test]
    fn test_parse_value_blob() {
        let mut reader = Cursor::new(vec![0x01, 0x02, 0x03, 0x04]);
        let value = parse_value(&mut reader, SerialType::Blob(4));
        assert_eq!(value, Value::Blob(vec![0x01, 0x02, 0x03, 0x04]));
    }

    #[test]
    fn test_parse_value_text() {
        let mut reader = Cursor::new(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]);
        let value = parse_value(&mut reader, SerialType::Text(5));
        assert_eq!(value, Value::Text("Hello".to_string()));
    }

    #[test]
    fn test_parse_value_null() {
        let mut reader = Cursor::new(vec![]);
        let value = parse_value(&mut reader, SerialType::Null);
        assert_eq!(value, Value::Null);
    }
}

impl Record {
    pub fn parse<T: Read>(reader: &mut T) -> Self {
        let (size, _) = read_varint(reader);
        let (row_id, _) = read_varint(reader);
        let header = RecordHeader { size, row_id };
        let column_header = ColumnHeader::parse(reader);
        let mut payload = vec![0; (size - column_header.size) as usize];
        reader
            .read_exact(&mut payload)
            .expect("failed to read payload from reader");
        let mut values = vec![];

        for serial_type in column_header.column_types.clone() {
            values.push(parse_value(reader, serial_type))
        }
        Record {
            header,
            column_header,
            values,
        }
    }
}

impl ColumnHeader {
    pub fn parse<T: Read>(reader: &mut T) -> Self {
        let (size, b) = read_varint(reader);
        let mut bytes_read = b;
        let mut column_types: Vec<SerialType> = vec![];

        while bytes_read < size {
            let (type_encoding, b) = read_varint(reader);
            bytes_read += b;
            column_types.push(SerialType::from(type_encoding));
        }

        return ColumnHeader { size, column_types };
    }
}

impl BTreeHeader {
    pub fn parse<T: Read>(reader: &mut T) -> Result<BTreeHeader, Error> {
        let mut buf = [0; 12];
        reader.read_exact(&mut buf)?;
        let page_type: BTreePageType = match buf[0] {
            0x02 => BTreePageType::InteriorIndex,
            0x05 => BTreePageType::InteriorTable,
            0x0a => BTreePageType::LeafIndex,
            0x0d => BTreePageType::LeafTable,
            _ => bail!("Invalid B-Tree Page Type"),
        };

        // The four-byte page number at offset 8 is the right-most pointer.
        // This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        let rightmost_pointer = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => Some(
                u32::from_be_bytes(buf[8..12].try_into().expect("incorrect range size")),
            ),
            BTreePageType::LeafIndex | BTreePageType::LeafTable => None,
        };

        Ok(BTreeHeader {
            page_type,
            first_free_block: u16::from_be_bytes([buf[1], buf[2]]),
            cell_count: u16::from_be_bytes([buf[3], buf[4]]),
            cell_content_offset: u16::from_be_bytes([buf[5], buf[6]]),
            fragmented_free_bytes: u8::from_be_bytes([buf[7]]),
            rightmost_pointer,
        })
    }
}
