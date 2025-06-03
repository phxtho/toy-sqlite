use std::io::{Read, Seek, SeekFrom};

use anyhow::Error;

use crate::{
    query_engine::{filter::create_record_filter, set::Set},
    serialisation::deserialize::Deserialize,
    sql_parser::parser::Comparison,
};

use super::btree::record::HasRecord;

pub struct Table<T: Deserialize> {
    pub cells: Vec<T>,
    pub columns: Option<Set<String>>,
}

impl<T: Deserialize> Table<T> {
    /// Generic function to use cell pointers to deserialize a collection of cells on a page
    /// i.e IndexInteriorCells,TableLeafCells etc.
    pub fn new<R: Seek + Read>(reader: &mut R, cell_pointers: &Vec<u16>) -> Self {
        let cells = cell_pointers
            .iter()
            .map(|cell_ptr| {
                let seek_pos = SeekFrom::Start(*cell_ptr as u64);
                reader
                    .seek(seek_pos)
                    .expect("failed to seek to cell pointer");
                T::deserialize(reader)
            })
            .collect();
        Table {
            cells,
            columns: None,
        }
    }
}

impl<T: Deserialize + HasRecord + Clone> Table<T> {
    pub fn filter_cells(
        &self,
        ordered_column_names: &Vec<String>,
        comparison: &Comparison,
    ) -> Result<Vec<T>, Error> {
        let record_predicate = create_record_filter(ordered_column_names, comparison)?;

        Ok(self
            .cells
            .iter()
            .filter(|item| record_predicate(item.record()))
            .cloned()
            .collect())
    }
}
