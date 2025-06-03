use anyhow::{anyhow, Error, Ok, Result};

use crate::{
    data_model::{
        btree::{
            page_header::PageType, record::HasRecord, table_interior_cell::TableInteriorCell,
            table_leaf_cell::TableLeafCell,
        },
        schema_record::SchemaRecord,
        table::Table,
    },
    sql_parser::parser::{Comparison, SelectQuery},
};

use super::{engine::QueryEngine, schema_object::SchemaObject};

impl<'a> QueryEngine<'a> {
    /// Find the table record in the schema table
    pub fn get_table_rec(&mut self, table_name: &str) -> Result<SchemaRecord, Error> {
        match self
            .pager
            .schema_table
            .cells
            .iter()
            .find(|rec| rec.tbl_name == table_name)
        {
            Some(rec) => return Ok(rec.clone()),
            _ => return Err(anyhow!("Couldn't find table: {}", table_name)),
        }
    }

    pub fn table_db_scan(
        &mut self,
        table: &SchemaObject,
        query: &SelectQuery,
    ) -> Result<Vec<TableLeafCell>> {
        let mut records: Vec<TableLeafCell> = vec![];
        self.recursive_db_scan(
            table.rootpage,
            &mut records,
            &table.columns,
            &query.where_clause,
        )?;
        Ok(records)
    }

    /// Traverses a BTree collecting records in the leaf nodes
    fn recursive_db_scan(
        &mut self,
        page_number: u32,
        records: &mut Vec<TableLeafCell>,
        ordered_column_names: &Vec<String>,
        comparison: &Option<Comparison>,
    ) -> Result<()> {
        let (page, mut buf) = self.pager.read_page(page_number)?;
        match page.header.page_type {
            PageType::TableInterior => {
                let interior_table = Table::<TableInteriorCell>::new(&mut buf, &page.cell_pointers);
                drop(buf);
                for cell in interior_table.cells {
                    self.recursive_db_scan(
                        cell.left_child,
                        records,
                        ordered_column_names,
                        comparison,
                    )?;
                }
                match page.header.rightmost_pointer {
                    Some(rightmost_pointer) => {
                        self.recursive_db_scan(
                            rightmost_pointer,
                            records,
                            ordered_column_names,
                            comparison,
                        )?;
                    }
                    _ => panic!("Interior table page header missing right most pointer"),
                }
                Ok(())
            }
            PageType::TableLeaf => {
                let mut table = Table::<TableLeafCell>::new(&mut buf, &page.cell_pointers);

                match comparison {
                    Some(cmp) => records.append(
                        &mut table
                            .filter_cells(&ordered_column_names, &cmp)
                            .expect("filtering failed"),
                    ),
                    None => records.append(&mut table.cells),
                };

                Ok(())
            }
            _ => panic!("Found an Index Page while traversing Table tree"),
        }
    }

    pub fn table_binary_search(
        &mut self,
        table: &SchemaObject,
        mut row_ids: Vec<u64>,
    ) -> Result<Vec<TableLeafCell>> {
        let mut records: Vec<TableLeafCell> = vec![];

        while row_ids.len() > 0 {
            self.recursive_binary_search(table.rootpage, &mut row_ids, &mut records)?;
        }

        Ok(records)
    }

    fn recursive_binary_search<'b>(
        &mut self,
        page_number: u32,
        queried_row_ids: &mut Vec<u64>,
        records: &'b mut Vec<TableLeafCell>,
    ) -> Result<()> {
        let (page, mut buf) = self.pager.read_page(page_number)?;

        let Some(row_id) = queried_row_ids.first().cloned() else {
            return Ok(());
        };

        match page.header.page_type {
            PageType::TableInterior => {
                let interior_table = Table::<TableInteriorCell>::new(&mut buf, &page.cell_pointers);
                for cell in &interior_table.cells {
                    if cell.row_id >= row_id {
                        self.recursive_binary_search(cell.left_child, queried_row_ids, records)?;
                    }
                }
                match page.header.rightmost_pointer {
                    Some(rightmost_pointer) => {
                        if interior_table.cells.last().unwrap().row_id <= row_id {
                            self.recursive_binary_search(
                                rightmost_pointer,
                                queried_row_ids,
                                records,
                            )
                        } else {
                            Ok(())
                        }
                    }
                    None => panic!("Interior table page header missing right most pointer"),
                }
            }
            PageType::TableLeaf => {
                let table = Table::<TableLeafCell>::new(&mut buf, &page.cell_pointers);

                // Linear search page for row-ids
                for r_id in queried_row_ids.clone() {
                    match table.cells.iter().find(|c| c.row_id() == r_id) {
                        Some(c) => records.push(c.clone()),
                        None => (),
                    };
                }
                // Remove found row ids
                let found_rows: Vec<u64> =
                    records.iter().map(|rec| rec.row_header.row_id).collect();
                queried_row_ids.retain(|r_id| !found_rows.contains(r_id));

                Ok(())
            }
            _ => panic!("Found an Index page while traversing a Table B+ Tree"),
        }
    }
}

#[cfg(test)]
mod query_engine_table_tests {}
