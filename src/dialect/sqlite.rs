use super::Dialect;

/// SQLite dialect implementation of the `Dialect` trait.
#[cfg(feature = "sqlite")]
pub struct SqliteDialect;

#[cfg(feature = "sqlite")]
impl Dialect for SqliteDialect {
    fn placeholder(_idx: usize) -> String {
        "?".to_string()
    }

    fn exists_tag_query(_idx: usize) -> String {
        "EXISTS (SELECT 1 FROM image_tags WHERE image_tags.image_hash = images.hash AND image_tags.tag_name = ?)".to_string()
    }

    fn ensure_image_statement() -> &'static str {
        "INSERT OR IGNORE INTO images (hash) VALUES (?)"
    }

    fn ensure_tag_statement() -> &'static str {
        "INSERT OR IGNORE INTO tags (name) VALUES (?)"
    }

    fn ensure_image_tag_statement() -> &'static str {
        "INSERT OR IGNORE INTO image_tags (image_hash, tag_name) VALUES (?, ?)"
    }

    fn query_image_statement(condition: String) -> String {
        format!("SELECT hash WHERE {}", condition)
    }

    fn query_tags_by_image_statement() -> &'static str {
        "SELECT tag_name FROM image_tags WHERE image_hash = ?"
    }

    fn delete_image_tag_statement() -> &'static str {
        "DELETE FROM image_tags WHERE image_hash = ? AND tag_name = ?"
    }

    fn delete_image_statement() -> &'static str {
        "DELETE FROM image WHERE hash = ?"
    }

    fn delete_tags_by_image_statement() -> &'static str {
        "DELETE FROM image_tags WHERE image_hash = ?"
    }
}
