use anyhow::{anyhow, Context, Error, Ok, Result};
use regex::Regex;

use crate::data_model::schema_record::SchemaRecord;

pub struct ColumnDefinition {
    pub name: String,
    pub type_def: String,
}

pub fn is_integer_primary_key(table_record: &SchemaRecord, col_idx: &usize) -> Result<bool> {
    let column_defintions = get_column_definitions(&table_record.sql)?;
    match column_defintions.get(*col_idx) {
        Some(col_def) => {
            if col_def.type_def.contains("primary key")
                && (col_def.type_def.contains("int") || col_def.type_def.contains("integer"))
            {
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
        _ => Err(anyhow!(
            "No column at index {} found on table {}",
            *col_idx,
            table_record.name
        )),
    }
}

#[cfg(test)]
mod is_integer_primary_key_tests {

    use super::*;
    use crate::data_model::schema_record::{DbObject, SchemaRecord};

    impl SchemaRecord {
        pub fn new(tbl_name: String, sql: String) -> Self {
            Self {
                db_object: DbObject::Table,
                name: "".to_string(),
                rootpage: 1,
                tbl_name,
                sql,
            }
        }
    }

    #[test]
    fn test_integer_primary_key() {
        let tbl_name = "test".to_string();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)".to_string();
        let table = SchemaRecord::new(tbl_name, sql);

        // Test that column index 0 is an integer primary key.
        let result = is_integer_primary_key(&table, &0).unwrap();
        assert!(result, "Expected column 0 to be an integer primary key");
    }

    #[test]
    fn test_integer_not_primary_key() {
        let tbl_name = "test".to_string();
        let sql = "CREATE TABLE TEST (id INTEGER, name TEXT)".to_string();
        let table = SchemaRecord::new(tbl_name, sql);

        let result = is_integer_primary_key(&table, &0).unwrap();
        assert!(
            !result,
            "Expected column 0 to not be an integer primary key"
        );
    }

    #[test]
    fn test_primary_key_not_integer() {
        let tbl_name = "test".to_string();
        let sql = "CREATE TABLE test (id TEXT PRIMARY KEY, name TEXT)".to_string();
        let table = SchemaRecord::new(tbl_name, sql);

        let result = is_integer_primary_key(&table, &0).unwrap();
        assert!(
            !result,
            "Expected column 0 to not be an integer primary key since it's not an integer"
        );
    }

    #[test]
    fn test_out_of_bounds() {
        let tbl_name = "test".to_string();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY)".to_string();
        let table = SchemaRecord::new(tbl_name, sql);

        let result = is_integer_primary_key(&table, &1);
        assert!(
            result.is_err(),
            "Expected an error when accessing a non-existent column index"
        );
    }
}

/// Extract the column definitions from a create table sql statement
pub fn get_column_definitions(create_table_sql: &str) -> Result<Vec<ColumnDefinition>> {
    // Normalize by removing extra spaces and line breaks
    let normalized_sql = create_table_sql
        .replace('\n', " ")
        .replace('\t', " ")
        .trim()
        .to_string();

    // Regular expression to find columns inside parentheses
    let re = Regex::new(r"\((.*)\)")?;
    let captures = re
        .captures(&normalized_sql)
        .context("Invalid CREATE TABLE syntax.")?;

    // Extract the columns string
    let column_definitions_str = captures.get(1).context("No columns found.")?.as_str();
    // Split columns by commas and trim whitespace
    let column_defintions: Vec<ColumnDefinition> = column_definitions_str
        .split(",")
        .map(|col_def| {
            let mut parts = col_def.trim().splitn(2, ' ');

            let name = parts.next().unwrap();
            let type_def = parts.next().unwrap_or("");

            ColumnDefinition {
                name: name.to_lowercase().to_string(),
                type_def: type_def.to_lowercase().to_string(),
            }
        })
        .collect();

    Ok(column_defintions)
}

pub fn get_column_names(create_table_sql: &str) -> Result<Vec<String>, Error> {
    let defintions: Vec<ColumnDefinition> = get_column_definitions(create_table_sql)?;
    let column_names = defintions
        .iter()
        .map(|col_definition| col_definition.name.clone())
        .collect();

    Ok(column_names)
}

pub fn find_column_index(ordered_column_names: &Vec<String>, name: &str) -> Result<usize, Error> {
    // Find the index of the given column name
    for (index, column_name) in ordered_column_names.iter().enumerate() {
        if column_name.eq_ignore_ascii_case(name) {
            return Ok(index);
        }
    }

    Err(anyhow!("Column '{}' not found.", name))
}

#[test]
fn test_find_column_index() {
    let create_table_sql = r#"
        CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            department VARCHAR(50)
        );
    "#;

    let columns: Vec<String> =
        get_column_names(create_table_sql).expect("couldn't find column names");

    let index = find_column_index(&columns, "name").unwrap();
    assert_eq!(index, 1);
}
