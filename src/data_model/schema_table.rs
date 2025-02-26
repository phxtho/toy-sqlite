use std::io::{Read, Seek};

use itertools::Itertools;

use crate::data_model::{schema_record::SchemaRecord, table::Table};

pub struct SchemaTable {
    pub records: Vec<SchemaRecord>,
}

impl SchemaTable {
    pub fn new<T: Seek + Read>(reader: &mut T, cell_pointers: Vec<u16>) -> Self {
        let table = Table::new(reader, cell_pointers);
        let records = table
            .records
            .iter()
            .map(|rec| SchemaRecord::from(rec.to_owned()))
            .collect_vec();
        SchemaTable { records }
    }
}
