use anyhow::{bail, Result};
use itertools::Itertools;
use regex::Regex;
use sqlite_starter_rust::btree_header::BTreeHeader;
use sqlite_starter_rust::btree_page::BTreePage;
use sqlite_starter_rust::db_header::Dbheader;
use sqlite_starter_rust::parsers::Parse;
use sqlite_starter_rust::record::Record;
use sqlite_starter_rust::schema_table::SchemaRecord;
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

    let mut file = File::open(&args[1])?;
    let db_header = Dbheader::parse(&mut file);
    let root_page = BTreePage::parse(&mut file);

    match command {
        ".dbinfo" => dbinfo(db_header, root_page),
        ".tables" => tables(root_page, &mut file),
        cmd if re_select_count.is_match(cmd) => {
            // Capture the table name from the SQL statement
            let caps = re_select_count.captures(cmd).unwrap();
            let table_name = caps.get(1).map_or("", |m| m.as_str());
            if table_name == "" {
                bail!("missing table name");
            }
            count(db_header, root_page, &mut file, table_name);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn read_table<T: Seek + Read>(reader: &mut T, cell_pointers: Vec<u16>) -> Vec<Record> {
    let mut records: Vec<Record> = vec![];
    for ptr in cell_pointers {
        let seek_pos = SeekFrom::Start(ptr as u64);
        reader
            .seek(seek_pos)
            .expect("failed to seek to cell pointer");
        let record = Record::parse(reader);
        records.push(record)
    }
    records
}

fn dbinfo(db_header: Dbheader, root_page: BTreePage) {
    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", root_page.header.cell_count);
}

fn tables(root_page: BTreePage, file: &mut File) {
    let records = read_table(file, root_page.cell_pointers);
    let table_names = records
        .iter()
        .map(|rec| SchemaRecord::from(rec.to_owned()))
        .filter(|rec| !rec.tbl_name.starts_with("sql"))
        .map(|r| r.tbl_name)
        .join(" ");
    println!("{}", table_names);
}

fn count(db_header: Dbheader, root_page: BTreePage, file: &mut File, table_name: &str) {
    let table = read_table(file, root_page.cell_pointers);
    let mut schema_table = table.iter().map(|rec| SchemaRecord::from(rec.to_owned()));

    match schema_table.find(|rec| rec.tbl_name == table_name) {
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
