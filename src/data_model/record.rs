use std::io::Read;

use crate::data_model::{
    column_header::ColumnHeader,
    record_header::RecordHeader,
    serial_value::{deserialize_value, SerialValue},
    serialisation::{read_varint, Deserialize},
};

#[derive(Clone)]
pub struct Record {
    pub header: RecordHeader,
    pub column_header: ColumnHeader,
    pub values: Vec<SerialValue>,
}

impl Deserialize for Record {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let (size, _) = read_varint(reader);
        let (row_id, _) = read_varint(reader);
        let column_header = ColumnHeader::deserialize(reader);
        let mut values = vec![];
        for serial_type in column_header.column_types.clone() {
            values.push(deserialize_value(reader, serial_type))
        }
        Record {
            header: RecordHeader { size, row_id },
            column_header,
            values,
        }
    }
}

#[cfg(test)]
mod parse_record_tests {
    use std::io::Cursor;

    use crate::data_model::{
        record::Record, serial_type::SerialType, serial_value::SerialValue,
        serialisation::Deserialize,
    };

    #[test]
    fn test_record_parsing() {
        // INSERT INTO sandwiches (name, length, count) VALUES ('Italian', 7.5, 2)
        let mut reader = Cursor::new(vec![
            0x15, 0x01, 0x05, 0x00, 0x1b, 0x07, 0x01, 0x49, 0x74, 0x61, 0x6c, 0x69, 0x61, 0x6e,
            0x40, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        ]);
        let record = Record::deserialize(&mut reader);
        assert_eq!(record.header.size, 21);
        assert_eq!(record.header.row_id, 1);
        assert_eq!(record.column_header.size, 5);
        // When an SQL table includes an INTEGER PRIMARY KEY column (which aliases the rowid) then that column appears in the record as a NULL value.
        assert_eq!(record.column_header.column_types[0], SerialType::Null);
        assert_eq!(record.column_header.column_types[1], SerialType::Text(7));
        assert_eq!(record.column_header.column_types[2], SerialType::Float64);
        assert_eq!(record.column_header.column_types[3], SerialType::Int8);
        assert_eq!(record.values[0], SerialValue::Null);
        assert_eq!(record.values[1], SerialValue::Text("Italian".to_string()));
        assert_eq!(record.values[2], SerialValue::Float(7.5));
        assert_eq!(record.values[3], SerialValue::Int(2));
    }
}
