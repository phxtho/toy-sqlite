use std::io::Read;

use crate::{btree_header::BTreeHeader, parsers::Parse};

pub struct BTreePage {
    pub header: BTreeHeader,
    pub cell_pointers: Vec<u16>,
}

impl Parse for BTreePage {
    fn parse<T: Read>(reader: &mut T) -> Self {
        let header = BTreeHeader::parse(reader);
        let cell_pointers = read_cell_pointer(reader, header.cell_count);
        BTreePage {
            header,
            cell_pointers,
        }
    }
}

fn read_cell_pointer<T: Read>(reader: &mut T, cell_count: u16) -> Vec<u16> {
    let mut cell_pointers: Vec<u16> = vec![];
    for _ in 0..cell_count {
        let mut buf = [0; 2];
        reader
            .read_exact(&mut buf)
            .expect("failed to read cell pointer");
        cell_pointers.push(u16::from_be_bytes(buf));
    }
    cell_pointers
}
