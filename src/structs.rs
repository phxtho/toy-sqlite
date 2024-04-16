use std::io::Read;

use anyhow::Error;

pub struct Dbheader {
    pub page_size: u16,
}

impl Dbheader {
    pub fn parse<T: Read>(reader: &mut T) -> Result<Dbheader, Error> {
        let mut buf = [0; 100];
        reader.read_exact(&mut buf)?;
        return Ok(Dbheader {
            page_size: u16::from_be_bytes([buf[16], buf[17]]),
        });
    }
}
#[derive(PartialEq, Debug)]
pub enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
#[allow(dead_code)]
pub struct BTreeHeader {
    pub page_type: BTreePageType,
    pub first_free_block: u16,
    pub cell_count: u16,
    pub cell_content_offset: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

pub struct RecordHeader {
    // size of the row excluding the bytes to represent the header
    pub size: u64,
    pub row_id: u64,
}

pub struct Record {
    pub header: RecordHeader,
    pub column_header: ColumnHeader,
    pub values: Vec<Value>,
}

pub struct ColumnHeader {
    pub size: u64, // size of the column header including this field
    pub column_types: Vec<SerialType>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SerialType {
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
    // return the number of bytes to represent this type sqlite file
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
            7 => SerialType::Float64,
            8 => SerialType::Zero,
            9 => SerialType::One,
            10 | 11 => panic!("Serial type for internal use"),
            value => {
                if value >= 12 && value % 2 == 0 {
                    SerialType::Blob(((value - 12) / 2) as usize)
                } else if value >= 13 && value % 2 == 1 {
                    SerialType::Text(((value - 13) / 2) as usize)
                } else {
                    panic!("Invalid serial type encoding {}", value);
                }
            }
        }
    }
}
#[derive(PartialEq, Debug)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}
