use crate::serialisation::{deserialize::Deserialize, varint::read_varint};

use super::{
    record::{HasRecord, Record},
    serial_value::SerialValue,
};

pub struct IndexInteriorCell {
    pub left_child: u32,
    pub size: u64,
    pub record: Record,
}

impl Deserialize for IndexInteriorCell {
    fn deserialize<T: std::io::Read>(reader: &mut T) -> Self {
        let mut buf = [0u8; 4];
        reader
            .read(&mut buf)
            .expect("Failed to read 4 bytes for left child pointer");
        let left_child = u32::from_be_bytes(buf);

        let (size, _) = read_varint(reader);
        let record = Record::deserialize(reader);

        Self {
            left_child,
            size,
            record,
        }
    }
}

impl HasRecord for IndexInteriorCell {
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
