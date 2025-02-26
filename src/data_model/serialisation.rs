use std::io::Read;

pub trait Deserialize {
    fn deserialize<T: Read>(reader: &mut T) -> Self;
}

pub fn read_varint<T: Read>(reader: &mut T) -> (u64, u64) {
    let mut read_more = true;
    let mut result: u64 = 0;
    let mut bytes_read = 0;

    while read_more {
        // read first byte
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).expect("failed to read varint");
        bytes_read += 1;

        // check high bit for continuation
        let high_bit = buf[0] & 0b1000_0000;
        read_more = high_bit != 0;

        // make space for next 7 bits
        result <<= 7;
        // isolate low 7 bits and add to result
        let low_bits = buf[0] & 0b0111_1111;
        result |= low_bits as u64;
    }

    return (result, bytes_read);
}

#[test]
fn test_read_varint() {
    let mut buf = std::io::Cursor::new(vec![0b0000_0001, 0b0000_0001]);
    assert_eq!(read_varint(&mut buf), (1, 1));
    assert_eq!(read_varint(&mut buf), (1, 1));
}

#[test]
fn test_read_varint_reading_zero() {
    let mut buf = std::io::Cursor::new(vec![0b0000]);
    assert_eq!(read_varint(&mut buf), (0, 1));
}

#[test]
fn test_read_varint_reading_continuation_bits() {
    let mut buf = std::io::Cursor::new(vec![0b10000111, 0b01101000]);
    assert_eq!(read_varint(&mut buf), (1000, 2));
}

use anyhow::{anyhow, Context, Error, Result};
use regex::Regex;

pub fn get_column_names(create_table_sql: &str) -> Result<Vec<String>, Error> {
    // Normalize by removing extra spaces and line breaks
    let normalized_sql = create_table_sql
        .replace('\n', " ")
        .replace('\t', " ")
        .trim()
        .to_string();

    // Regular expression to find columns inside parentheses
    let re = Regex::new(r"\((.*)\)")?;
    let captures = re
        .captures(&normalized_sql)
        .context("Invalid CREATE TABLE syntax.")?;

    // Extract the columns string
    let column_definitions = captures.get(1).context("No columns found.")?.as_str();

    // Split columns by commas and trim whitespace
    let column_names = column_definitions
        .split(',')
        .map(|col_definition| {
            col_definition
                .trim()
                // Assume the column definition starts with the name
                .split_whitespace()
                .next()
                .expect("Invalid column definition.")
                .to_string()
        })
        .collect();

    Ok(column_names)
}

pub fn find_column_index(ordered_column_names: &Vec<String>, name: &str) -> Result<usize, Error> {
    // Find the index of the given column name
    for (index, column_name) in ordered_column_names.iter().enumerate() {
        if column_name.eq_ignore_ascii_case(name) {
            return Ok(index);
        }
    }

    Err(anyhow!("Column '{}' not found.", name))
}

#[test]
fn test_find_column_index() {
    let create_table_sql = r#"
        CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            department VARCHAR(50)
        );
    "#;

    let columns: Vec<String> =
        get_column_names(create_table_sql).expect("couldn't find column names");

    match find_column_index(&columns, "name") {
        Ok(index) => println!("The index of 'name' is: {}", index),
        Err(e) => eprintln!("Error: {}", e),
    }
}
