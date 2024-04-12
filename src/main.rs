use anyhow::{bail, Error, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

struct Dbheader {
    page_size: u16,
}

impl Dbheader {
    fn parse<T: Read>(reader: &mut T) -> Result<Dbheader, Error> {
        let mut buf = [0; 100];
        reader.read_exact(&mut buf)?;
        return Ok(Dbheader {
            page_size: u16::from_be_bytes([buf[16], buf[17]]),
        });
    }
}

enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
#[allow(dead_code)]
struct BTreeHeader {
    page_type: BTreePageType,
    first_free_block: u16,
    cell_count: u16,
    cell_content_offset: u16,
    fragmented_free_bytes: u8,
    rightmost_pointer: Option<u32>,
}

impl BTreeHeader {
    fn parse<T: Read>(reader: &mut T) -> Result<BTreeHeader, Error> {
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

struct RecordHeader {
    size: u64,
    row_id: u64,
}

struct Record {
    header: RecordHeader,
    column_header: ColumnHeader,
    values: Vec<Value>,
}

impl Record {
    fn parse<T: Read>(reader: &mut T) -> Self {
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

#[derive(Clone)]
enum SerialType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float64,
    Zero, // schema format 4 or higher
    One,  // schema format 4 or higher
    Blob(usize),
    Text(usize),
}

impl SerialType {
    pub fn size(s: Self) -> usize {
        match s {
            SerialType::Int8 => 1,
            SerialType::Int16 => 2,
            SerialType::Int24 => 3,
            SerialType::Int32 => 4,
            SerialType::Int48 => 6,
            SerialType::Int64 => 8,
            SerialType::Float64 => 8,
            SerialType::Zero | SerialType::One => 0,
            SerialType::Blob(size) => size,
            SerialType::Text(size) => size,
            _ => panic!("Invalid serial type"),
        }
    }
}

impl From<u64> for SerialType {
    fn from(value: u64) -> Self {
        match value {
            0 => SerialType::Null,
            1 => SerialType::Int8,
            2 => SerialType::Int16,
            3 => SerialType::Int24,
            4 => SerialType::Int32,
            5 => SerialType::Int48,
            6 => SerialType::Int64,
            8 => SerialType::Zero,
            9 => SerialType::One,
            value => {
                if value >= 12 && value % 2 == 0 {
                    SerialType::Blob(((value - 12) / 2) as usize)
                } else if value >= 13 && value % 2 == 1 {
                    SerialType::Text(((value - 13) / 2) as usize)
                } else {
                    panic!("Invalide serial type encoding {}", value);
                }
            }
        }
    }
}

enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

struct ColumnHeader {
    size: u64, // size of the column header including this field
    column_types: Vec<SerialType>,
}

impl ColumnHeader {
    fn parse<T: Read>(reader: &mut T) -> Self {
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

fn read_varint<T: Read>(reader: &mut T) -> (u64, u64) {
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

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];

    let mut file = File::open(&args[1])?;
    let db_header = Dbheader::parse(&mut file)?;
    let page_header = BTreeHeader::parse(&mut file)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db_header.page_size);
            println!("number of tables: {}", page_header.cell_count);
        }
        "table" => {
            let mut cell_pointers: Vec<u16> = vec![];
            for _ in 0..page_header.cell_count {
                let mut buf = [0; 2];
                file.read_exact(&mut buf)?;
                cell_pointers.push(u16::from_be_bytes(buf));
            }

            let mut records: Vec<Record> = vec![];
            for ptr in cell_pointers {
                let seek_pos = SeekFrom::Start(ptr as u64);
                file.seek(seek_pos).expect("failed to seek in file");
                let record = Record::parse(&mut file);
                records.push(record)
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
