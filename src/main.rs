use anyhow::{bail, Result};
use itertools::Itertools;
use toy_sqlite::data_model::btree::page::Page;
use toy_sqlite::data_model::table::Table;
use toy_sqlite::data_model::{db_header::Dbheader, schema_record::SchemaRecord};

use std::fs::File;
use toy_sqlite::pager::pager::Pager;
use toy_sqlite::query_engine::engine::QueryEngine;
use toy_sqlite::sql_parser::{
    lexer::lexer,
    parser::{Parser, SelectQuery},
};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = args[2].as_str();

    let mut file = File::open(&args[1])?;
    let pager = Pager::new(&mut file)?;

    match command {
        ".dbinfo" => dbinfo(pager.db_header, pager.root_page),
        ".tables" => tables(pager.schema_table),
        cmd if !cmd.is_empty() => {
            let mut query_engine = QueryEngine::new(pager);
            let result = query_engine.run_query(parse_sql(cmd)).unwrap();
            println!("{}", result);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn dbinfo(db_header: Dbheader, root_page: Page) {
    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", root_page.header.cell_count);
}

fn tables(schema_table: Table<SchemaRecord>) {
    let table_names = schema_table
        .cells
        .iter()
        // sqlite_schema table has alternate names such as 'sql_master' or 'sqlite_temp_schema'
        // https://www.sqlite.org/schematab.html#alternative_names
        .filter(|rec| !rec.tbl_name.starts_with("sql"))
        .map(|r| r.tbl_name.clone())
        .join(" ");
    println!("{}", table_names);
}

fn parse_sql(query: &str) -> SelectQuery {
    let tokens = lexer(query);
    let mut parser = Parser::new(tokens);
    parser.parse()
}
