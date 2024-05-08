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
            let mut byte_array = [0u8; MAX_SIZE];
            byte_array[MAX_SIZE - buffer_size..MAX_SIZE].copy_from_slice(&buf[..buffer_size]);
            Value::Int(i64::from_be_bytes(byte_array))
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
mod parse_values_tests {
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
        let column_header = ColumnHeader::parse(reader);
        let mut values = vec![];
        for serial_type in column_header.column_types.clone() {
            values.push(parse_value(reader, serial_type))
        }
        Record {
            header: RecordHeader { size, row_id },
            column_header,
            values,
        }
    }
}
#[cfg(test)]
mod parse_record_tests {
    use std::io::Cursor;

    use crate::structs::{Record, SerialType, Value};
    #[test]
    fn test_record_parsing() {
        // INSERT INTO sandwiches (name, length, count) VALUES ('Italian', 7.5, 2)
        let mut reader = Cursor::new(vec![
            0x15, 0x01, 0x05, 0x00, 0x1b, 0x07, 0x01, 0x49, 0x74, 0x61, 0x6c, 0x69, 0x61, 0x6e,
            0x40, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        ]);
        let record = Record::parse(&mut reader);
        assert_eq!(record.header.size, 21);
        assert_eq!(record.header.row_id, 1);
        assert_eq!(record.column_header.size, 5);
        // When an SQL table includes an INTEGER PRIMARY KEY column (which aliases the rowid) then that column appears in the record as a NULL value.
        assert_eq!(record.column_header.column_types[0], SerialType::Null);
        assert_eq!(record.column_header.column_types[1], SerialType::Text(7));
        assert_eq!(record.column_header.column_types[2], SerialType::Float64);
        assert_eq!(record.column_header.column_types[3], SerialType::Int8);
        assert_eq!(record.values[0], Value::Null);
        assert_eq!(record.values[1], Value::Text("Italian".to_string()));
        assert_eq!(record.values[2], Value::Float(7.5));
        assert_eq!(record.values[3], Value::Int(2));
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
        let mut buf = [0; 1];
        reader.read_exact(&mut buf)?;
        let page_type: BTreePageType = match buf[0] {
            0x02 => BTreePageType::InteriorIndex,
            0x05 => BTreePageType::InteriorTable,
            0x0a => BTreePageType::LeafIndex,
            0x0d => BTreePageType::LeafTable,
            _ => bail!("Invalid B-Tree Page Type"),
        };

        let remaining_header_size = match page_type {
            BTreePageType::InteriorTable | BTreePageType::InteriorIndex => 11,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => 7,
        };

        let mut buf = vec![0; remaining_header_size];
        reader.read_exact(&mut buf)?;

        // The four-byte page number at offset 8 is the right-most pointer.
        // This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        let rightmost_pointer = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => {
                Some(u32::from_be_bytes(
                    buf[remaining_header_size - 4..remaining_header_size]
                        .try_into()
                        .expect("incorrect range size"),
                ))
            }
            BTreePageType::LeafIndex | BTreePageType::LeafTable => None,
        };

        Ok(BTreeHeader {
            page_type,
            first_free_block: u16::from_be_bytes([buf[0], buf[1]]),
            cell_count: u16::from_be_bytes([buf[2], buf[3]]),
            cell_content_offset: u16::from_be_bytes([buf[4], buf[5]]),
            fragmented_free_bytes: u8::from_be_bytes([buf[6]]),
            rightmost_pointer,
        })
    }
}

#[cfg(test)]
mod parse_btreeheader_tests {
    use std::io::Cursor;

    use crate::structs::{BTreeHeader, BTreePageType};
    #[test]
    fn test_parsing_leaftable_header() {
        let mut reader = Cursor::new(vec![0x0d, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0]);
        let btree_header = BTreeHeader::parse(&mut reader).expect("Couldn't parse BTreeHeader");
        assert_eq!(btree_header.page_type, BTreePageType::LeafTable);
        assert_eq!(btree_header.cell_count, 3);
        assert_eq!(btree_header.rightmost_pointer, None);
    }

    #[test]
    fn test_parsing_interiortable_header() {
        let mut reader = Cursor::new(vec![
            0x05, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
        ]);
        let btree_header = BTreeHeader::parse(&mut reader).expect("Couldn't parse BTreeHeader");
        assert_eq!(btree_header.page_type, BTreePageType::InteriorTable);
        assert_eq!(btree_header.cell_count, 3);
        assert_eq!(btree_header.rightmost_pointer, Some(1));
    }
}
