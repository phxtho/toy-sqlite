use crate::{
    data_model::{
        btree::{
            index_interior_cell::IndexInteriorCell, index_leaf_cell::IndexLeafCell,
            page_header::PageType,
        },
        schema_record::DbObject,
        table::Table,
    },
    sql_parser::parser::{Comparison, SelectQuery},
};

use super::{engine::QueryEngine, filter::greater_than, schema_object::SchemaObject};

impl<'a> QueryEngine<'a> {
    /*
    Finds an index for the columns
    For multi-column indexes querying a single column it will match for the first column in the index
    else all columns being queried need to match the index definition
     */
    pub fn find_index(&self, query: &SelectQuery) -> Option<SchemaObject> {
        let queried_columns: Vec<&String> = match &query.where_clause {
            Some(where_clause) => vec![&where_clause.column],
            None => return None,
        };
        self.pager
            .schema_table
            .cells
            .iter()
            .filter(|s_rec| s_rec.db_object == DbObject::Index && s_rec.tbl_name == query.table)
            .map(|s_rec| SchemaObject::from(s_rec.clone()))
            .find(|index| queried_columns.len() == 1 && *queried_columns[0] == index.columns[0])
    }

    /// collect row-ids which match the comparison on the index
    pub fn index_binary_search(
        &mut self,
        page_number: u32,
        comparison: &Comparison,
        index_records: &mut Vec<IndexLeafCell>,
        index: &SchemaObject,
    ) {
        let (page, mut buf) = self
            .pager
            .read_page(page_number)
            .expect("failed to read index page");

        match page.header.page_type {
            PageType::IndexInterior => {
                let interior_table = Table::<IndexInteriorCell>::new(&mut buf, &page.cell_pointers);
                drop(buf);
                for cell in &interior_table.cells {
                    // todo: instead of just assuming the indexed columm is the first value find it properly
                    let serial_value = cell.record.values.first().expect("failed to get first");
                    match greater_than(serial_value, &comparison.value) {
                        Ok(is_greater) => {
                            if is_greater {
                                self.index_binary_search(
                                    cell.left_child,
                                    comparison,
                                    index_records,
                                    index,
                                );
                            }
                        }
                        Err(_) => todo!("Deal with comparison error"),
                    }
                }
                match page.header.rightmost_pointer {
                    Some(rightmost_pointer) => {
                        // todo: error handling
                        let last_key = &interior_table
                            .cells
                            .last()
                            .unwrap()
                            .record
                            .values
                            .first()
                            .unwrap();

                        if !greater_than(last_key, &comparison.value).unwrap() {
                            self.index_binary_search(
                                rightmost_pointer,
                                comparison,
                                index_records,
                                index,
                            );
                        }
                    }
                    _ => panic!("Interior table page header missing right most pointer"),
                }
            }
            PageType::IndexLeaf => {
                let table = Table::<IndexLeafCell>::new(&mut buf, &page.cell_pointers);
                index_records.append(&mut table.filter_cells(&index.columns, comparison).unwrap());
            }
            PageType::TableLeaf | PageType::TableInterior => {
                panic!("Found a Table page while traversing an Index BTree")
            }
        }
    }
}

#[cfg(test)]
mod query_engine_index_tests {
    use std::{fs::File, path::Path};

    use crate::{
        pager::pager::Pager,
        sql_parser::parser::{Column, Operator},
    };

    use super::*;

    #[test]
    fn test_index_binary_search() {
        let path = Path::new("companies.db");
        let mut file = File::open(path).expect("Failed to open companies.db");

        let pager = Pager::new(&mut file).expect("Failed to initialize pager");
        let mut engine = QueryEngine::new(pager);

        let query = SelectQuery {
            columns: vec![Column::All],
            table: "companies".to_string(),
            where_clause: Some(Comparison {
                column: "country".to_string(),
                operator: Operator::Equals,
                value: "rwanda".to_string(),
            }),
        };

        let country_index = engine.find_index(&query).unwrap();

        let mut index_records: Vec<IndexLeafCell> = vec![];
        engine.index_binary_search(
            country_index.rootpage,
            &query.where_clause.unwrap(),
            &mut index_records,
            &country_index,
        );
        assert_eq!(index_records.len(), 288);
    }
}
