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

        return Ok(BTreeHeader {
            page_type,
            first_free_block: u16::from_be_bytes([buf[1], buf[2]]),
            cell_count: u16::from_be_bytes([buf[3], buf[4]]),
            cell_content_offset: u16::from_be_bytes([buf[5], buf[6]]),
            fragmented_free_bytes: u8::from_be_bytes([buf[7]]),
            rightmost_pointer,
        });
    }
}
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

struct RecordHeader {
    size: u64, // size of the record header including this field
    column_types: Vec<SerialType>,
}

impl RecordHeader {
    fn parse<T: Read>(reader: &mut T) {
        let (size, b) = read_varint(reader);
        let mut bytes_read = b;
        let mut column_types: Vec<SerialType> = vec![];

        while bytes_read < size {
            let (type_encoding, b) = read_varint(reader);
            bytes_read += b;
            column_types.push(SerialType::from(type_encoding));
        }

        RecordHeader { size, column_types };
    }
}

struct Record {
    header: RecordHeader,
    body: Vec<u8>,
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
    let dbheader = Dbheader::parse(&mut file)?;
    let btreeheader = BTreeHeader::parse(&mut file)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", dbheader.page_size);
            println!("number of tables: {}", btreeheader.cell_count);
        }
        "table" => {
            let mut cell_pointers: Vec<u16> = vec![];
            for _ in 0..btreeheader.cell_count {
                let mut buf = [0; 2];
                file.read_exact(&mut buf)?;
                cell_pointers.push(u16::from_be_bytes(buf));
            }

            for ptr in cell_pointers {
                let seek_pos = SeekFrom::Start(ptr as u64);
                file.seek(seek_pos).expect("failed to seek in file");
                let record = Record::parse(&mut file);
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
