use std::io::Read;

use crate::data_model::serialisation::Deserialize;

#[derive(PartialEq, Debug)]
pub enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

pub struct BTreeHeader {
    pub page_type: BTreePageType,
    pub first_free_block: u16,
    pub cell_count: u16, // how many records exist on this page
    pub cell_content_offset: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

impl Deserialize for BTreeHeader {
    fn deserialize<T: Read>(reader: &mut T) -> BTreeHeader {
        let mut buf = [0; 1];
        reader
            .read_exact(&mut buf)
            .expect("failed to read BTreePageType");
        let page_type: BTreePageType = match buf[0] {
            0x02 => BTreePageType::InteriorIndex,
            0x05 => BTreePageType::InteriorTable,
            0x0a => BTreePageType::LeafIndex,
            0x0d => BTreePageType::LeafTable,
            _ => panic!("Invalid B-Tree Page Type"),
        };

        let remaining_header_size = match page_type {
            BTreePageType::InteriorTable | BTreePageType::InteriorIndex => 11,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => 7,
        };

        let mut buf = vec![0; remaining_header_size];
        reader
            .read_exact(&mut buf)
            .expect("failed to read BTreePageHeader");

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

        BTreeHeader {
            page_type,
            first_free_block: u16::from_be_bytes([buf[0], buf[1]]),
            cell_count: u16::from_be_bytes([buf[2], buf[3]]),
            cell_content_offset: u16::from_be_bytes([buf[4], buf[5]]),
            fragmented_free_bytes: u8::from_be_bytes([buf[6]]),
            rightmost_pointer,
        }
    }
}

#[cfg(test)]
mod parse_btreeheader_tests {
    use std::io::Cursor;

    use crate::data_model::{
        btree_header::{BTreeHeader, BTreePageType},
        serialisation::Deserialize,
    };

    #[test]
    fn test_deserializing_leaftable_header() {
        let mut reader = Cursor::new(vec![0x0d, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0]);
        let btree_header = BTreeHeader::deserialize(&mut reader);
        assert_eq!(btree_header.page_type, BTreePageType::LeafTable);
        assert_eq!(btree_header.cell_count, 3);
        assert_eq!(btree_header.rightmost_pointer, None);
    }

    #[test]
    fn test_parsing_interiortable_header() {
        let mut reader = Cursor::new(vec![
            0x05, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
        ]);
        let btree_header = BTreeHeader::deserialize(&mut reader);
        assert_eq!(btree_header.page_type, BTreePageType::InteriorTable);
        assert_eq!(btree_header.cell_count, 3);
        assert_eq!(btree_header.rightmost_pointer, Some(1));
    }
}
