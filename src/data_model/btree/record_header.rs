use std::io::Read;

use crate::{
    data_model::btree::serial_type::SerialType,
    serialisation::{deserialize::Deserialize, varint::read_varint},
};

#[derive(Clone)]
pub struct RecordHeader {
    pub size: u64, // size of the column header including this field
    pub column_types: Vec<SerialType>,
}

impl Deserialize for RecordHeader {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let (size, b) = read_varint(reader);
        let mut bytes_read = b;
        let mut column_types: Vec<SerialType> = vec![];

        while bytes_read < size {
            let (type_encoding, b) = read_varint(reader);
            bytes_read += b;
            column_types.push(SerialType::from(type_encoding));
        }

        assert_eq!(bytes_read, size);

        return RecordHeader { size, column_types };
    }
}
