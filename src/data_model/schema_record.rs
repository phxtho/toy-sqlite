use crate::data_model::{record::Record, serial_value::SerialValue};

// The sqlite_schema table contains one record for each table, index, view, and trigger (collectively "objects") in the database schema,
#[derive(Debug, Clone)]
pub struct SchemaRecord {
    pub db_object: DbObject,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: i64, // pages are 1-indexed
    pub sql: String,
}

#[derive(Debug, Clone)]
pub enum DbObject {
    Table,
    Index,
    View,
    Trigger,
}

impl From<Record> for SchemaRecord {
    fn from(record: Record) -> Self {
        assert_eq!(record.values.len(), 5);
        let db_object = match &record.values[0] {
            SerialValue::Text(t) => match t.as_str() {
                "table" => DbObject::Table,
                "index" => DbObject::Index,
                "view" => DbObject::View,
                "trigger" => DbObject::Trigger,
                _ => panic!("Unknow DbObject type"),
            },
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let name = match &record.values[1] {
            SerialValue::Text(name) => name.to_owned(),
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let tbl_name = match &record.values[2] {
            SerialValue::Text(tbl_name) => tbl_name.to_owned(),
            _ => panic!("expected column value[0] to be of type Text"),
        };
        let rootpage = match &record.values[3] {
            SerialValue::Int(rootpage) => rootpage.to_owned(),
            _ => panic!("expected column value[3] to be of type Int"),
        };
        let sql = match &record.values[4] {
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
