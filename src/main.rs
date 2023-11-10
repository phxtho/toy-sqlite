use anyhow::{bail, Error, Result};
use std::fs::File;
use std::io::prelude::*;

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
    num_cells: u16,
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
            num_cells: u16::from_be_bytes([buf[3], buf[4]]),
            cell_content_offset: u16::from_be_bytes([buf[5], buf[6]]),
            fragmented_free_bytes: u8::from_be_bytes([buf[7]]),
            rightmost_pointer,
        });
    }
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
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;

            let dbheader = Dbheader::parse(&mut file)?;
            println!("database page size: {}", dbheader.page_size);

            let btreeheader = BTreeHeader::parse(&mut file)?;
            println!("number of tables: {}", btreeheader.num_cells);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
