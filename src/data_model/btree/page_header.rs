use std::{fmt::Display, io::Read};

use crate::serialisation::deserialize::Deserialize;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum PageType {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

impl Display for PageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableLeaf => write!(f, "Table Leaf"),
            Self::TableInterior => write!(f, "Table Interior"),
            Self::IndexLeaf => write!(f, "Index Leaf"),
            Self::IndexInterior => write!(f, "Index Interior"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_free_block: u16,
    pub cell_count: u16, // how many records exist on this page
    pub cell_content_offset: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

impl Deserialize for PageHeader {
    fn deserialize<T: Read>(reader: &mut T) -> PageHeader {
        let mut buf = [0; 1];
        reader
            .read_exact(&mut buf)
            .expect("failed to read BTreePageType");
        let page_type: PageType = match buf[0] {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            _ => panic!("Invalid B-Tree Page Type"),
        };

        let remaining_header_size = match page_type {
            PageType::TableInterior | PageType::IndexInterior => 11,
            PageType::IndexLeaf | PageType::TableLeaf => 7,
        };

        let mut buf = vec![0; remaining_header_size];
        reader
            .read_exact(&mut buf)
            .expect("failed to read BTreePageHeader");

        // The four-byte page number at offset 8 is the right-most pointer.
        // This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        let rightmost_pointer = match page_type {
            PageType::IndexInterior | PageType::TableInterior => Some(u32::from_be_bytes(
                buf[remaining_header_size - 4..remaining_header_size]
                    .try_into()
                    .expect("incorrect range size"),
            )),
            PageType::IndexLeaf | PageType::TableLeaf => None,
        };

        PageHeader {
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

    use crate::{
        data_model::btree::page_header::{PageHeader, PageType},
        serialisation::deserialize::Deserialize,
    };

    #[test]
    fn test_deserializing_leaftable_header() {
        let mut reader = Cursor::new(vec![0x0d, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0]);
        let page_header = PageHeader::deserialize(&mut reader);
        assert_eq!(page_header.page_type, PageType::TableLeaf);
        assert_eq!(page_header.cell_count, 3);
        assert_eq!(page_header.rightmost_pointer, None);
    }

    #[test]
    fn test_parsing_interiortable_header() {
        let mut reader = Cursor::new(vec![
            0x05, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
        ]);
        let page_header = PageHeader::deserialize(&mut reader);
        assert_eq!(page_header.page_type, PageType::TableInterior);
        assert_eq!(page_header.cell_count, 3);
        assert_eq!(page_header.rightmost_pointer, Some(1));
    }
}
