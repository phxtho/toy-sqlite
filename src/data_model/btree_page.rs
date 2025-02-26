use std::io::Read;

use crate::data_model::{btree_header::BTreeHeader, serialisation::Deserialize};

pub struct BTreePage {
    pub header: BTreeHeader,
    pub cell_pointers: Vec<u16>,
}

impl Deserialize for BTreePage {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let header = BTreeHeader::deserialize(reader);
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
