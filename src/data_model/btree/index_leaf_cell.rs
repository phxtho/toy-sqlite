use crate::serialisation::{deserialize::Deserialize, varint::read_varint};

use super::{
    record::{HasRecord, Record},
    serial_value::SerialValue,
};

#[derive(Clone)]
pub struct IndexLeafCell {
    pub size: u64,
    pub record: Record, // pub overflow_page: u32,
}

impl Deserialize for IndexLeafCell {
    fn deserialize<T: std::io::Read>(reader: &mut T) -> Self {
        let (size, _) = read_varint(reader);
        let record = Record::deserialize(reader);

        Self { size, record }
    }
}

impl HasRecord for IndexLeafCell {
    fn record(&self) -> &Record {
        &self.record
    }
    fn row_id(&self) -> u64 {
        match self
            .record
            .values
            .last()
            .expect("Index record should end with row id")
        {
            SerialValue::Int(row_id) => *row_id as u64,
            _ => panic!("Final value in index cell should be row_id as Integer"),
        }
    }
}

#[cfg(test)]
mod index_leaf_cell_test {
    #[test]
    fn test_deserialisation() {}
}
