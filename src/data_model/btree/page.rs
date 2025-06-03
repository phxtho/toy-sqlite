use std::io::Read;

use crate::serialisation::deserialize::Deserialize;

use super::page_header::PageHeader;

#[derive(Clone)]
pub struct Page {
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
}

impl Deserialize for Page {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let header = PageHeader::deserialize(reader);
        let cell_pointers = read_cell_pointer(reader, header.cell_count);
        Page {
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
