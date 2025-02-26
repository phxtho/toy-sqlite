use std::io::{Read, Seek, SeekFrom};

use crate::data_model::{record::Record, serialisation::Deserialize};

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
            let record = Record::deserialize(reader);
            records.push(record)
        }
        Table { records }
    }
}
