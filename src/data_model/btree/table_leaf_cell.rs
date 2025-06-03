use std::io::Read;

use crate::serialisation::{deserialize::Deserialize, varint::read_varint};

use super::record::{HasRecord, Record};

#[derive(Clone)]
pub struct RowHeader {
    // size of the row excluding the bytes to represent the header
    pub size: u64,
    pub row_id: u64,
}

impl Deserialize for RowHeader {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let (size, _) = read_varint(reader);
        let (row_id, _) = read_varint(reader);
        RowHeader { size, row_id }
    }
}

#[derive(Clone)]
pub struct TableLeafCell {
    pub row_header: RowHeader,
    pub record: Record,
}

impl Deserialize for TableLeafCell {
    fn deserialize<T: Read>(reader: &mut T) -> Self {
        let row_header = RowHeader::deserialize(reader);
        let record = Record::deserialize(reader);
        TableLeafCell { row_header, record }
    }
}

impl HasRecord for TableLeafCell {
    fn record(&self) -> &Record {
        &self.record
    }

    fn row_id(&self) -> u64 {
        self.row_header.row_id
    }
}

#[cfg(test)]
mod parse_record_tests {
    use std::io::Cursor;

    use crate::data_model::btree::{
        serial_type::SerialType, serial_value::SerialValue, table_leaf_cell::TableLeafCell,
    };
    use crate::serialisation::deserialize::Deserialize;

    #[test]
    fn test_record_parsing() {
        // INSERT INTO sandwiches (name, length, count) VALUES ('Italian', 7.5, 2)
        let mut reader = Cursor::new(vec![
            0x15, 0x01, 0x05, 0x00, 0x1b, 0x07, 0x01, 0x49, 0x74, 0x61, 0x6c, 0x69, 0x61, 0x6e,
            0x40, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        ]);
        let cell = TableLeafCell::deserialize(&mut reader);
        assert_eq!(cell.row_header.size, 21);
        assert_eq!(cell.row_header.row_id, 1);
        assert_eq!(cell.record.header.size, 5);
        // When an SQL table includes an INTEGER PRIMARY KEY column (which aliases the rowid) then that column appears in the record as a NULL value.
        assert_eq!(cell.record.header.column_types[0], SerialType::Null);
        assert_eq!(cell.record.header.column_types[1], SerialType::Text(7));
        assert_eq!(cell.record.header.column_types[2], SerialType::Float64);
        assert_eq!(cell.record.header.column_types[3], SerialType::Int8);
        assert_eq!(cell.record.values[0], SerialValue::Null);
        assert_eq!(
            cell.record.values[1],
            SerialValue::Text("Italian".to_string())
        );
        assert_eq!(cell.record.values[2], SerialValue::Float(7.5));
        assert_eq!(cell.record.values[3], SerialValue::Int(2));
    }
}
