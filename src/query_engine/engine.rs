use anyhow::{Error, Result};
use itertools::Itertools;

use crate::{
    data_model::btree::{
        index_leaf_cell::IndexLeafCell, page_header::PageType, record::HasRecord,
        serial_value::SerialValue, table_leaf_cell::TableLeafCell,
    },
    pager::pager::Pager,
    sql_parser::parser::{AggregateFn, Column, Comparison, SelectQuery},
};

use super::{
    column::{find_column_index, is_integer_primary_key},
    schema_object::SchemaObject,
    set::Set,
};

pub struct QueryEngine<'a> {
    pub pager: Pager<'a>,
}

impl<'a> QueryEngine<'a> {
    pub fn new(pager: Pager<'a>) -> Self {
        Self { pager }
    }

    pub fn run_query(&mut self, query: SelectQuery) -> Result<String, Error> {
        let table_record = self.get_table_rec(query.table.as_str())?;
        let table: SchemaObject = SchemaObject::from(table_record.clone());

        if query
            .columns
            .contains(&Column::Aggregation(AggregateFn::CountAll))
        {
            let (page, _) = self
                .pager
                .read_page(table.rootpage)
                .expect("couldn't find page");

            if page.header.page_type == PageType::TableLeaf {
                return Ok(page.header.cell_count.to_string());
            } else {
                unimplemented!("Count records on tables that span multiple pages")
            }
        } else {
            // get the array index of the queried columns
            let mut queried_col_idxs: Set<usize> = Set::new();
            query.columns.iter().for_each(|column| match column {
                Column::Regular(col_name) => {
                    let idx = find_column_index(&table.columns, col_name)
                        .expect("couldn't find column name");
                    queried_col_idxs.push(idx);
                }
                Column::All => {
                    // add the remaining columns to the col indx vec
                    for col_name in &table.columns {
                        let idx = find_column_index(&table.columns, &col_name)
                            .expect("couldn't find column name");
                        queried_col_idxs.push(idx);
                    }
                }
                _ => (),
            });

            let records: Vec<TableLeafCell> = match &query.where_clause {
                Some(comparison) => match self.find_index(&query) {
                    Some(index) => self.search_with_index(table, index, &comparison)?,
                    None => self.table_db_scan(&table, &query)?,
                },
                None => self.table_db_scan(&table, &query)?,
            };

            // collect the queried columns from the records
            let col_values = records
                .iter()
                .map(|cell| {
                    queried_col_idxs
                        .iter()
                        .map(|col_idx| {
                            let v = cell
                                .record
                                .values
                                .get(*col_idx)
                                .expect("failed to find column index");
                            // When a table includes an INTEGER PRIMARY KEY column then that column appears in the record as a NULL value and aliases rowid.
                            if *v == SerialValue::Null
                                && is_integer_primary_key(&table_record, col_idx).unwrap()
                            {
                                SerialValue::Int(cell.row_id() as i64)
                            } else {
                                v.clone()
                            }
                        })
                        .join("|")
                })
                .join("\n");
            return Ok(col_values);
        }
    }

    fn search_with_index(
        &mut self,
        table: SchemaObject,
        index: SchemaObject,
        comparison: &Comparison,
    ) -> Result<Vec<TableLeafCell>> {
        // Binary search index
        let mut matching_index_leaf_cells: Vec<IndexLeafCell> = vec![];
        self.index_binary_search(
            index.rootpage,
            &comparison,
            &mut matching_index_leaf_cells,
            &index,
        );

        let rows_to_find = matching_index_leaf_cells
            .iter()
            .map(|c| c.row_id())
            .collect();
        // Binary search table
        return self.table_binary_search(&table, rows_to_find);
    }
}

#[cfg(test)]
mod execution_engine_tests {
    use super::*;
    use crate::sql_parser::parser::{AggregateFn, Column, Operator};
    use std::{fs::File, path::Path};

    #[test]
    fn test_select_count() {
        let path = Path::new("sample.db");
        let mut file = File::open(path).expect("Failed to open sample.db");

        // Create an instance of ExecutionEngine
        let pager = Pager::new(&mut file).expect("Failed to initialize pager");
        let mut engine = QueryEngine::new(pager);

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
        let pager = Pager::new(&mut file).expect("Failed to initialize pager");
        let mut engine = QueryEngine::new(pager);

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

    #[test]
    fn test_searching_by_index() {
        let path = Path::new("companies.db");
        let mut file = File::open(path).expect("Failed to open companies.db");

        let pager = Pager::new(&mut file).expect("Failed to initialize pager");
        let mut engine = QueryEngine::new(pager);
        let query = SelectQuery {
            columns: vec![Column::All],
            table: "companies".into(),
            where_clause: Some(Comparison {
                column: "country".into(),
                operator: Operator::Equals,
                value: "rwanda".into(),
            }),
        };

        let table = SchemaObject::from(engine.get_table_rec("companies").unwrap());
        let index = SchemaObject::from(engine.find_index(&query).unwrap());

        let matching_recs = engine
            .search_with_index(table, index, &query.where_clause.unwrap())
            .unwrap();

        assert_eq!(matching_recs.len(), 288);
    }
}
