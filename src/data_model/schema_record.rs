use crate::{
    data_model::btree::{serial_value::SerialValue, table_leaf_cell::TableLeafCell},
    serialisation::deserialize::Deserialize,
};

// The sqlite_schema table contains one record for each table, index, view, and trigger (collectively "objects") in the database schema,
// https://www.sqlite.org/schematab.html
#[derive(Debug, Clone)]
pub struct SchemaRecord {
    pub db_object: DbObject,
    pub name: String,     // the name of the underlying object
    pub tbl_name: String, // name of the referenced table
    pub rootpage: u32,    // pages are 1-indexed
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DbObject {
    Table,
    Index,
    View,
    Trigger,
}

impl Deserialize for SchemaRecord {
    fn deserialize<T: std::io::Read>(reader: &mut T) -> Self {
        SchemaRecord::from(TableLeafCell::deserialize(reader))
    }
}

impl From<TableLeafCell> for SchemaRecord {
    fn from(cell: TableLeafCell) -> Self {
        assert_eq!(cell.record.values.len(), 5);
        let db_object = match &cell.record.values[0] {
            SerialValue::Text(t) => match t.as_str() {
                "table" => DbObject::Table,
                "index" => DbObject::Index,
                "view" => DbObject::View,
                "trigger" => DbObject::Trigger,
                _ => panic!("Unknow DbObject type"),
            },
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let name = match &cell.record.values[1] {
            SerialValue::Text(name) => name.to_owned(),
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let tbl_name = match &cell.record.values[2] {
            SerialValue::Text(tbl_name) => tbl_name.to_owned(),
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let rootpage = match &cell.record.values[3] {
            SerialValue::Int(rootpage) => rootpage.to_owned() as u32,
            _ => panic!("expected column value[3] to be of type Int"),
        };
        let sql = match &cell.record.values[4] {
            SerialValue::Text(sql) => sql.to_owned(),
            _ => panic!("expected column value[4] to be of type Text"),
        };
        SchemaRecord {
            db_object,
            name,
            tbl_name,
            rootpage,
            sql,
        }
    }
}
