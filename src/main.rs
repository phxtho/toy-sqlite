use anyhow::{bail, Error, Result};
use itertools::Itertools;
use regex::Regex;
use sqlite_starter_rust::schema_table::SchemaRecord;
use sqlite_starter_rust::structs::{BTreeHeader, Dbheader, Record};
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
    let db_header = Dbheader::parse(&mut file)?;
    let root_page_header = BTreeHeader::parse(&mut file)?;
    let cell_pointers = read_cell_pointer(&mut file, root_page_header.cell_count)?;

    match command {
        ".dbinfo" => {
            println!("database page size: {}", db_header.page_size);
            println!("number of tables: {}", root_page_header.cell_count);
        }
        ".tables" => {
            let records = read_table(&mut file, cell_pointers);
            let table_names = records
                .iter()
                .map(|rec| SchemaRecord::from(rec.to_owned()))
                .filter(|rec| !rec.tbl_name.starts_with("sql"))
                .map(|r| r.tbl_name)
                .join(" ");
            println!("{}", table_names);
        }
        cmd if re_select_count.is_match(cmd) => {
            // Capture the table name from the SQL statement
            let caps = re_select_count.captures(cmd).unwrap();
            let table_name = caps.get(1).map_or("", |m| m.as_str());
            if table_name == "" {
                bail!("missing table name");
            }
            // Read the Schema table
            let table = read_table(&mut file, cell_pointers);
            let mut schema_table = table.iter().map(|rec| SchemaRecord::from(rec.to_owned()));
            // Find the record with table name
            match schema_table.find(|rec| rec.tbl_name == table_name) {
                Some(rec) => {
                    // Go to it's root page or page ?
                    let pos = (rec.rootpage - 1) as u64 * db_header.page_size as u64;
                    file.seek(SeekFrom::Start(pos)).expect("couldn't find page");
                    // Read page
                    let page_header = BTreeHeader::parse(&mut file)?;
                    // Output cell count of the page or parse the table and output number of records ?
                    println!("{}", page_header.cell_count);
                }
                None => {}
            };
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn read_cell_pointer<T: Seek + Read>(stream: &mut T, cell_count: u16) -> Result<Vec<u16>, Error> {
    // Should probably seek to immediatly after the pageheader
    // or parse this at the same time as the page header
    let mut cell_pointers: Vec<u16> = vec![];
    for _ in 0..cell_count {
        let mut buf = [0; 2];
        stream.read_exact(&mut buf)?;
        cell_pointers.push(u16::from_be_bytes(buf));
    }
    Ok(cell_pointers)
}

fn read_table<T: Seek + Read>(stream: &mut T, cell_pointers: Vec<u16>) -> Vec<Record> {
    let mut records: Vec<Record> = vec![];
    for ptr in cell_pointers {
        let seek_pos = SeekFrom::Start(ptr as u64);
        stream
            .seek(seek_pos)
            .expect("failed to seek to cell pointer");
        let record = Record::parse(stream);
        records.push(record)
    }
    records
}
