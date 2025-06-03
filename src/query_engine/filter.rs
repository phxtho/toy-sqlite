use anyhow::{Error, Result};

use crate::{
    data_model::btree::{record::Record, serial_value::SerialValue},
    sql_parser::parser::{Comparison, Operator},
};

use super::column::find_column_index;

pub fn check_equality(serial_value: &SerialValue, comparison_value: &String) -> Result<bool> {
    let equality: bool = match serial_value {
        SerialValue::Null => comparison_value.eq_ignore_ascii_case("null"),
        SerialValue::Int(val) => *val == comparison_value.parse()?,
        SerialValue::Float(val) => *val == comparison_value.parse()?,
        SerialValue::Text(val) => val == comparison_value,
        SerialValue::Blob(_) => todo!("equality of blobs"),
    };
    Ok(equality)
}

pub fn greater_than(serial_value: &SerialValue, comparison_value: &String) -> Result<bool> {
    let greater: bool = match serial_value {
        SerialValue::Null => false,
        SerialValue::Int(val) => *val >= comparison_value.parse()?,
        SerialValue::Float(val) => *val >= comparison_value.parse()?,
        SerialValue::Text(val) => val >= comparison_value,
        SerialValue::Blob(_) => todo!("ordering of blobs"),
    };
    Ok(greater)
}

/// Get a closure that can filter records according to the comparison
pub fn create_record_filter<'a>(
    ordered_column_names: &'a Vec<String>,
    comparison: &'a Comparison,
) -> Result<impl Fn(&Record) -> bool + 'a, Error> {
    // get index of column
    let idx = find_column_index(ordered_column_names, &comparison.column)?;
    // Return a closure that can be used for filtering
    Ok(move |rec: &Record| {
        if let Some(serial_value) = rec.values.get(idx) {
            match comparison.operator {
                Operator::Equals => {
                    check_equality(serial_value, &comparison.value).unwrap_or(false)
                }
            }
        } else {
            false
        }
    })
}

pub fn filter_items(
    items: &Vec<Record>,
    ordered_column_names: &Vec<String>,
    comparison: &Comparison,
) -> Result<Vec<Record>, Error> {
    let record_predicate = create_record_filter(ordered_column_names, comparison)?;

    Ok(items
        .iter()
        .filter(|r| record_predicate(r))
        .cloned()
        .collect())
}

#[cfg(test)]
mod apply_filter_test {
    use super::*;
    use crate::{
        data_model::btree::{
            record::Record, record_header::RecordHeader, serial_type::SerialType,
            serial_value::SerialValue,
        },
        sql_parser::parser::{Comparison, Operator},
    };

    #[test]
    fn test_apply_filter() {
        let column_name = "numbers".to_string();
        let ordered_column_names: Vec<String> = vec![column_name.clone()];
        let recs = vec![Record {
            header: RecordHeader {
                size: 2,
                column_types: vec![SerialType::Float64],
            },
            values: vec![SerialValue::Float(34.)],
        }];

        let cmp = Comparison {
            value: "34".to_string(),
            column: column_name,
            operator: Operator::Equals,
        };

        let filtered_recs = filter_items(&recs, &ordered_column_names, &cmp).unwrap();
        assert_eq!(filtered_recs.iter().len(), 1)
    }

    #[test]
    fn test_apply_filter_removes_value() {
        let column_name = "numbers".to_string();
        let ordered_column_names: Vec<String> = vec![column_name.clone()];
        let recs = vec![Record {
            header: RecordHeader {
                size: 2,
                column_types: vec![SerialType::Float64],
            },
            values: vec![SerialValue::Float(34.)],
        }];

        let cmp = Comparison {
            value: "1".to_string(),
            column: column_name,
            operator: Operator::Equals,
        };

        let filtered_recs = filter_items(&recs, &ordered_column_names, &cmp).unwrap();
        assert_eq!(filtered_recs.iter().len(), 0)
    }
}
