use anyhow::{bail, Result};
use itertools::Itertools;
use sqlite_starter_rust::data_model::{
    btree_page::BTreePage, db_header::Dbheader, schema_table::SchemaTable,
    serialisation::Deserialize,
};

use sqlite_starter_rust::execution_engine::select::ExecutionEngine;
use sqlite_starter_rust::sql_parser::{
    lexer::lexer,
    parser::{Parser, SelectQuery},
};
use std::fs::File;

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
    let db_header = Dbheader::deserialize(&mut file);
    let root_page = BTreePage::deserialize(&mut file);

    match command {
        ".dbinfo" => dbinfo(db_header, root_page),
        ".tables" => tables(root_page, &mut file),
        cmd if !cmd.is_empty() => {
            let mut execution_engine = ExecutionEngine::new(db_header, &mut file, root_page);
            let result = execution_engine.run_query(parse_sql(cmd)).unwrap();
            println!("{}", result);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn dbinfo(db_header: Dbheader, root_page: BTreePage) {
    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", root_page.header.cell_count);
}

fn tables(root_page: BTreePage, file: &mut File) {
    let schema_table = SchemaTable::new(file, root_page.cell_pointers);
    let table_names = schema_table
        .records
        .iter()
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
