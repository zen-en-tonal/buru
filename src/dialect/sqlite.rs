use super::Dialect;

/// SQLite dialect implementation of the `Dialect` trait.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn placeholder(_idx: usize) -> String {
        "?".to_string()
    }
}
