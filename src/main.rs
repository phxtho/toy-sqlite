use anyhow::{bail, Result};
use itertools::Itertools;
use regex::Regex;
use sqlite_starter_rust::btree_header::BTreeHeader;
use sqlite_starter_rust::btree_page::BTreePage;
use sqlite_starter_rust::db_header::Dbheader;
use sqlite_starter_rust::parsers::Parse;
use sqlite_starter_rust::schema_table::SchemaTable;
use sqlite_starter_rust::table::Table;
use sqlite_starter_rust::utils::find_column_index;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

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

    let re_select_count = Regex::new(r"(?i)^SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)$").unwrap();
    let re_select = Regex::new(r"(?i)^SELECT\s+(\w+)\s+FROM\s+(\w+)$").unwrap();
    let mut file = File::open(&args[1])?;
    let db_header = Dbheader::parse(&mut file);
    let root_page = BTreePage::parse(&mut file);

    match command {
        ".dbinfo" => dbinfo(db_header, root_page),
        ".tables" => tables(root_page, &mut file),
        cmd if re_select_count.is_match(cmd) => {
            let caps = re_select_count.captures(cmd).unwrap();
            let table_name = caps.get(1).map_or("", |m| m.as_str());
            select_count(db_header, root_page, &mut file, table_name);
        }
        cmd if re_select.is_match(cmd) => {
            let caps = re_select.captures(cmd).unwrap();
            let col_name = caps.get(1).map_or("", |m| m.as_str());
            let table_name = caps.get(2).map_or("", |m| m.as_str());
            select_column(db_header, root_page, &mut file, table_name, col_name);
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

fn select_count(db_header: Dbheader, root_page: BTreePage, file: &mut File, table_name: &str) {
    let schema_table = SchemaTable::new(file, root_page.cell_pointers);
    match schema_table
        .records
        .iter()
        .find(|rec| rec.tbl_name == table_name)
    {
        Some(rec) => {
            let pos = (rec.rootpage - 1) as u64 * db_header.page_size as u64;
            file.seek(SeekFrom::Start(pos)).expect("couldn't find page");
            let page_header = BTreeHeader::parse(file);
            println!("{}", page_header.cell_count);
        }
        None => {
            println!("couldn't find table name '{}'", table_name)
        }
    };
}

fn select_column(
    db_header: Dbheader,
    root_page: BTreePage,
    file: &mut File,
    table_name: &str,
    col_name: &str,
) {
    let schema_table = SchemaTable::new(file, root_page.cell_pointers);
    match schema_table
        .records
        .iter()
        .find(|rec| rec.tbl_name == table_name)
    {
        Some(rec) => {
            let col_idx =
                find_column_index(rec.sql.as_str(), col_name).expect("couldn't find column name");

            // Jump to page with table
            let page_pointer = (rec.rootpage - 1) as u64 * db_header.page_size as u64;
            file.seek(SeekFrom::Start(page_pointer))
                .expect("couldn't find page");
            let page = BTreePage::parse(file);
            // add the page offset to the cell pointers
            let cell_pointers = page
                .cell_pointers
                .iter()
                .map(|p| p + page_pointer as u16)
                .collect_vec();
            let table = Table::new(file, cell_pointers);

            // find the column values in the table
            let col_values = table
                .records
                .iter()
                .map(|rec| {
                    rec.values
                        .get(col_idx)
                        .expect("failed to find column index")
                })
                .join("\n");
            println!("{}", col_values);
        }
        None => {
            println!("couldn't find table name '{}'", table_name)
        }
    };
}
