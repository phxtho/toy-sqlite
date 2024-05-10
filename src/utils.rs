use std::io::Read;

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

use regex::Regex;
use std::error::Error;

pub fn find_column_index(
    create_table_sql: &str,
    column_name: &str,
) -> Result<usize, Box<dyn Error>> {
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
        .ok_or("Invalid CREATE TABLE syntax.")?;

    // Extract the columns string
    let columns_str = captures.get(1).ok_or("No columns found.")?.as_str();

    // Split columns by commas and trim whitespace
    let columns: Vec<&str> = columns_str.split(',').map(|col| col.trim()).collect();

    // Find the index of the given column name
    for (index, col) in columns.iter().enumerate() {
        // Assume the column definition starts with the name
        let col_name = col
            .split_whitespace()
            .next()
            .ok_or("Invalid column definition.")?;

        if col_name.eq_ignore_ascii_case(column_name) {
            return Ok(index);
        }
    }

    Err(format!("Column '{}' not found.", column_name).into())
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

    match find_column_index(create_table_sql, "name") {
        Ok(index) => println!("The index of 'name' is: {}", index),
        Err(e) => eprintln!("Error: {}", e),
    }
}
