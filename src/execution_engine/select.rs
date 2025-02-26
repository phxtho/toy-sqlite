use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

use anyhow::{anyhow, Error, Ok};
use itertools::Itertools;

use crate::{
    data_model::{
        btree_header::BTreeHeader,
        btree_page::BTreePage,
        db_header::Dbheader,
        record::Record,
        schema_record::SchemaRecord,
        schema_table::SchemaTable,
        serial_value::SerialValue,
        serialisation::{find_column_index, get_column_names, Deserialize},
        table::Table,
    },
    sql_parser::parser::{AggregateFn, Column, Comparison, Operator, SelectQuery},
};

pub struct ExecutionEngine<'a> {
    db_header: Dbheader,
    file: &'a mut File,
    root_page: BTreePage,
}

/// insert a unique item to vec while maintaing insertion order
fn insert_unique(vec: &mut Vec<usize>, idx: &usize) {
    if !vec.contains(&idx) {
        vec.push(*idx);
    }
}

#[cfg(test)]
mod insert_unique_value_tests {
    use crate::execution_engine::select::insert_unique;

    #[test]
    fn test_insert_unique_rejects_duplicate() {
        let mut v: Vec<usize> = (0..3).collect();
        insert_unique(&mut v, &2);
        assert_eq!(v.len(), 3);
        assert_eq!(v.last().unwrap(), &2)
    }

    #[test]
    fn test_insert_unique_accepts_new_value() {
        let mut v: Vec<usize> = (0..3).collect();
        insert_unique(&mut v, &3);
        assert_eq!(v.len(), 4);
        assert_eq!(v.last().unwrap(), &3)
    }
}

fn check_equality(serial_value: &SerialValue, comparison_value: &String) -> Result<bool, Error> {
    let equality: bool = match serial_value {
        SerialValue::Null => comparison_value.eq_ignore_ascii_case("null"),
        SerialValue::Int(val) => *val == comparison_value.parse()?,
        SerialValue::Float(val) => *val == comparison_value.parse()?,
        SerialValue::Text(val) => val == comparison_value,
        SerialValue::Blob(_) => todo!(),
    };
    Ok(equality)
}

/// filter out records (in place) according to the comparison
pub fn apply_comparison(
    records: &mut Vec<Record>,
    ordered_column_names: Vec<String>,
    comparison: Comparison,
) -> Result<(), Error> {
    // get index of column
    let idx = find_column_index(&ordered_column_names, &comparison.column)?;

    records.retain(|rec| {
        let serial_value = rec
            .values
            .get(idx)
            .expect("No value at expected column index");
        match comparison.operator {
            Operator::Equals => {
                check_equality(serial_value, &comparison.value).expect("Error in equality check")
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod apply_comparison_test {
    use crate::{
        data_model::{
            column_header::ColumnHeader, record::Record, record_header::RecordHeader,
            serial_type::SerialType, serial_value::SerialValue,
        },
        execution_engine::select::apply_comparison,
        sql_parser::parser::{Comparison, Operator},
    };

    #[test]
    fn test_apply_comparison() {
        let column_name = "numbers".to_string();
        let ordered_column_names: Vec<String> = vec![column_name.clone()];
        let mut recs = vec![Record {
            header: RecordHeader { row_id: 0, size: 0 },
            column_header: ColumnHeader {
                size: 0,
                column_types: vec![SerialType::Float64],
            },
            values: vec![SerialValue::Float(34.)],
        }];

        let cmp = Comparison {
            value: "34".to_string(),
            column: column_name,
            operator: Operator::Equals,
        };

        apply_comparison(&mut recs, ordered_column_names, cmp).unwrap();
        assert_eq!(recs.len(), 1)
    }
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(db_header: Dbheader, file: &'a mut File, root_page: BTreePage) -> Self {
        Self {
            db_header,
            file,
            root_page,
        }
    }

    /// Find the table record in the schema table
    fn get_table_rec(&mut self, table_name: &str) -> Result<SchemaRecord, Error> {
        let schema_table = SchemaTable::new(self.file, self.root_page.cell_pointers.clone());
        match schema_table
            .records
            .iter()
            .find(|rec| rec.tbl_name == table_name)
        {
            Some(rec) => return Ok(rec.clone()),
            None => return Err(anyhow!("Couldn't find table: {}", table_name)),
        }
    }

    pub fn run_query(&mut self, query: SelectQuery) -> Result<String, Error> {
        let table_record = self.get_table_rec(query.table.as_str())?;
        let table_location = (table_record.rootpage - 1) as u64 * self.db_header.page_size as u64;
        self.file
            .seek(SeekFrom::Start(table_location))
            .expect("couldn't find page");

        if query
            .columns
            .contains(&Column::Aggregation(AggregateFn::CountAll))
        {
            let page_header = BTreeHeader::deserialize(self.file);
            return Ok(page_header.cell_count.to_string());
        } else {
            let page = BTreePage::deserialize(self.file);

            let table_cols = get_column_names(&table_record.sql)?;
            let mut queried_col_idxs: Vec<usize> = vec![];
            query.columns.iter().for_each(|column| match column {
                Column::Regular(col_name) => {
                    let idx = find_column_index(&table_cols, col_name)
                        .expect("couldn't find column name");
                    insert_unique(&mut queried_col_idxs, &idx);
                }
                Column::All => {
                    // add the remaining columns to the col indx vec
                    for col_name in &table_cols {
                        let idx = find_column_index(&table_cols, &col_name)
                            .expect("couldn't find column name");
                        insert_unique(&mut queried_col_idxs, &idx);
                    }
                }
                _ => (),
            });

            // add the page offset to the cell pointers
            let cell_pointers = page
                .cell_pointers
                .iter()
                .map(|p| p + table_location as u16)
                .collect_vec();
            let mut table = Table::new(self.file, cell_pointers);

            // filter records
            match query.where_clause {
                Some(where_clause) => {
                    apply_comparison(&mut table.records, table_cols, where_clause)?;
                }
                _ => (),
            }

            // find the column values in the table
            let col_values = table
                .records
                .iter()
                .map(|rec| {
                    queried_col_idxs
                        .iter()
                        .map(|col_idx| {
                            let v = rec
                                .values
                                .get(*col_idx)
                                .expect("failed to find column index");
                            // When a table includes an INTEGER PRIMARY KEY column then that column appears in the record as a NULL value and aliases rowid.
                            match v {
                                &SerialValue::Null => SerialValue::Int(rec.header.row_id as i64),
                                _ => v.clone(),
                            }
                        })
                        .join("|")
                })
                .join("\n");
            return Ok(col_values);
        }
    }
}

#[cfg(test)]
mod execution_engine_tests {
    use super::*; // Import everything from the parent module
    use crate::sql_parser::parser::{AggregateFn, Column};
    use std::path::Path;

    #[test]
    fn test_select_count() {
        let path = Path::new("sample.db");
        let mut file = File::open(path).expect("Failed to open sample.db");

        // Create an instance of ExecutionEngine
        let db_header = Dbheader::deserialize(&mut file);
        let root_page = BTreePage::deserialize(&mut file);
        let mut engine = ExecutionEngine::new(db_header, &mut file, root_page);

        let query = SelectQuery {
            table: "apples".to_string(),
            columns: vec![Column::Aggregation(AggregateFn::CountAll)],
            where_clause: None,
        };

        // Run the query
        let result = engine.run_query(query).unwrap();
        assert_eq!(result, "4");
    }

    #[test]
    fn test_select_all() {
        let path = Path::new("sample.db");
        let mut file = File::open(path).expect("Failed to open sample.db");

        // Create an instance of ExecutionEngine
        let db_header = Dbheader::deserialize(&mut file);
        let root_page = BTreePage::deserialize(&mut file);
        let mut engine = ExecutionEngine::new(db_header, &mut file, root_page);

        let query = SelectQuery {
            table: "apples".to_string(),
            columns: vec![Column::All],
            where_clause: None,
        };

        // Run query
        let result = engine.run_query(query).unwrap();
        let expected_result: String = "1|Granny Smith|Light Green\n2|Fuji|Red\n3|Honeycrisp|Blush Red\n4|Golden Delicious|Yellow"
            .to_string();
        assert_eq!(result, expected_result)
    }
}
