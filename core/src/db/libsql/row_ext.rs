use std::backtrace::Backtrace;

use anyhow::{Result, anyhow};
use deadpool_libsql::libsql;

pub fn get<T>(row: &libsql::Row, column: &str) -> Result<T>
where
    T: FromSqlValue,
{
    for idx in 0..row.column_count() {
        if let Some(name) = row.column_name(idx) {
            if name.eq_ignore_ascii_case(column) {
                let value = row.get_value(idx)?;
                return T::from_sql_value(value, column)
                    .map_err(|err| err.context(format!("row dump: {}", dump_row(row))));
            }
        }
    }

    Err(anyhow!("column `{}` not found in row", column))
}

pub trait FromSqlValue: Sized {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self>;
}

impl FromSqlValue for String {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Text(text) => Ok(text),
            libsql::Value::Null => Err(anyhow!(
                "column `{}` expected text but was NULL\n{}",
                column,
                Backtrace::capture()
            )),
            _ => Err(anyhow!("column `{}` expected text value", column)),
        }
    }
}

impl FromSqlValue for Option<String> {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Null => Ok(None),
            libsql::Value::Text(text) => Ok(Some(text)),
            _ => Err(anyhow!("column `{}` expected text value", column)),
        }
    }
}

impl FromSqlValue for i64 {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Integer(num) => Ok(num),
            libsql::Value::Null => {
                Err(anyhow!("column `{}` expected integer but was NULL", column))
            }
            _ => Err(anyhow!("column `{}` expected integer value", column)),
        }
    }
}

impl FromSqlValue for Option<i64> {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Null => Ok(None),
            libsql::Value::Integer(num) => Ok(Some(num)),
            _ => Err(anyhow!("column `{}` expected integer value", column)),
        }
    }
}

impl FromSqlValue for Vec<u8> {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Blob(data) => Ok(data),
            libsql::Value::Null => Err(anyhow!("column `{}` expected blob but was NULL", column)),
            _ => Err(anyhow!("column `{}` expected blob value", column)),
        }
    }
}

impl FromSqlValue for Option<Vec<u8>> {
    fn from_sql_value(value: libsql::Value, column: &str) -> Result<Self> {
        match value {
            libsql::Value::Null => Ok(None),
            libsql::Value::Blob(data) => Ok(Some(data)),
            _ => Err(anyhow!("column `{}` expected blob value", column)),
        }
    }
}

fn dump_row(row: &libsql::Row) -> String {
    let mut parts = Vec::new();
    for idx in 0..row.column_count() {
        let name = row.column_name(idx).unwrap_or("<unnamed>");
        let value = match row.get_value(idx) {
            Ok(libsql::Value::Null) => "NULL".to_string(),
            Ok(libsql::Value::Integer(v)) => format!("Integer({v})"),
            Ok(libsql::Value::Real(v)) => format!("Real({v})"),
            Ok(libsql::Value::Text(v)) => format!("Text({v})"),
            Ok(libsql::Value::Blob(_)) => "Blob(<bytes>)".to_string(),
            Err(_) => "<?>".to_string(),
        };
        parts.push(format!("{name}={value}"));
    }
    parts.join(", ")
}
