use std::io::Read;

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
            let mut buf = vec![0u8; SerialType::size(serial_type)];
            reader.read_exact(&mut buf);
            Value::Int(i64::from_be_bytes(
                buf.try_into().expect("couldn't convert vec to array"),
            ))
        }
        SerialType::Zero => Value::Int(0),
        SerialType::One => Value::Int(1),
        SerialType::Float64 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf);
            Value::Float(f64::from_be_bytes(buf))
        }
        SerialType::Blob(size) => {
            let mut buf = vec![0u8; size];
            reader.read_exact(&mut buf);
            Value::Blob(buf)
        }
        SerialType::Text(size) => {
            let mut buf = vec![0u8; size];
            reader.read_exact(&mut buf);
            let out_str = String::from_utf8(buf).expect("failed to parse Text to string");
            Value::Text(out_str)
        }
        SerialType::Null => Value::Null,
    }
}

#[test]
fn test_parse_value_i64() {}

#[test]
fn test_parse_value_f64() {}

#[test]
fn test_parse_value_text() {}

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
