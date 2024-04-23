use anyhow::{bail, Result};
use sqlite_starter_rust::structs::{BTreeHeader, Dbheader, Record, Value};
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
    let command = &args[2];

    let mut file = File::open(&args[1])?;
    let db_header = Dbheader::parse(&mut file)?;
    let page_header = BTreeHeader::parse(&mut file)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db_header.page_size);
            println!("number of tables: {}", page_header.cell_count);
        }
        ".table" => {
            let mut cell_pointers: Vec<u16> = vec![];
            for _ in 0..page_header.cell_count {
                let mut buf = [0; 2];
                file.read_exact(&mut buf)?;
                cell_pointers.push(u16::from_be_bytes(buf));
            }

            let mut records: Vec<Record> = vec![];
            for ptr in cell_pointers {
                let seek_pos = SeekFrom::Start(ptr as u64);
                file.seek(seek_pos).expect("failed to seek in file");
                let record = Record::parse(&mut file);
                records.push(record)
            }

            // sqlite_schema(type,name,tbl_name,rootpage,sql)
            let mut tbl_names: Vec<String> = Vec::new();
            for r in records {
                match &r.values[2] {
                    &Value::Text(ref tbl_name) => {
                        if !tbl_name.starts_with("sql") {
                            tbl_names.push(tbl_name.to_string())
                        }
                    }
                    _ => bail!("Expected tbl_name value type to be Text"),
                }
            }

            println!("{}", tbl_names.join("\t"));
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
