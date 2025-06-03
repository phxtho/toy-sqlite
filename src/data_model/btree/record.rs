use crate::serialisation::deserialize::Deserialize;

use super::{
    record_header::RecordHeader,
    serial_value::{deserialize_value, SerialValue},
};

#[derive(Clone)]
pub struct Record {
    pub header: RecordHeader,
    pub values: Vec<SerialValue>,
}

impl Deserialize for Record {
    fn deserialize<T: std::io::Read>(reader: &mut T) -> Self {
        let header = RecordHeader::deserialize(reader);
        let values = header
            .column_types
            .iter()
            .map(|serial_type| deserialize_value(reader, serial_type.clone()))
            .collect();

        Self { header, values }
    }
}

pub trait HasRecord {
    fn record(&self) -> &Record;
    fn row_id(&self) -> u64;
}
