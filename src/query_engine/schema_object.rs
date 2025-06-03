use crate::data_model::schema_record::SchemaRecord;

use super::column::get_column_names;

/// Basically Schema Record but the sql creation field has been parsed
pub struct SchemaObject {
    pub rootpage: u32,
    pub tbl_name: String,
    pub name: String,
    // column names are ordered
    pub columns: Vec<String>,
}

impl From<SchemaRecord> for SchemaObject {
    fn from(value: SchemaRecord) -> Self {
        Self {
            name: value.name,
            rootpage: value.rootpage,
            tbl_name: value.tbl_name,
            columns: get_column_names(&value.sql).expect("couldn't get column names"),
        }
    }
}
