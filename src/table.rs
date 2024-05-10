use std::io::{Read, Seek, SeekFrom};

use crate::{parsers::Parse, record::Record};

pub struct Table {
    pub records: Vec<Record>,
}

impl Table {
    pub fn new<T: Seek + Read>(reader: &mut T, cell_pointers: Vec<u16>) -> Self {
        let mut records: Vec<Record> = vec![];
        for ptr in cell_pointers {
            let seek_pos = SeekFrom::Start(ptr as u64);
            reader
                .seek(seek_pos)
                .expect("failed to seek to cell pointer");
            let record = Record::parse(reader);
            records.push(record)
        }
        Table { records }
    }
}
