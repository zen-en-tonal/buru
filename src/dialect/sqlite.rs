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
        "EXISTS (SELECT 1 FROM image_tags WHERE image_tags.image_hash = image_with_metadata.hash AND image_tags.tag_name = ?)".to_string()
    }

    fn ensure_image_statement() -> &'static str {
        "INSERT OR IGNORE INTO images (hash) VALUES (?)"
    }

    fn ensure_tag_statement() -> &'static str {
        "INSERT OR IGNORE INTO tags (name) VALUES (?)"
    }

    fn ensure_metadata_statement() -> &'static str {
        r#"INSERT OR IGNORE INTO image_metadatas
        (image_hash, width, height, format, color_type, file_size, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)"#
    }

    fn ensure_image_tag_statement() -> &'static str {
        "INSERT OR IGNORE INTO image_tags (image_hash, tag_name) VALUES (?, ?)"
    }

    fn query_image_statement(condition: String) -> String {
        format!("SELECT hash FROM image_with_metadata {}", condition)
    }

    fn query_tags_by_image_statement() -> &'static str {
        "SELECT tag_name FROM image_tags WHERE image_hash = ?"
    }

    fn query_metadata_statement() -> &'static str {
        "SELECT * FROM image_metadatas WHERE image_hash = ?"
    }

    fn query_source_statement() -> &'static str {
        "SELECT source FROM images WHERE hash = ?"
    }

    fn delete_image_tag_statement() -> &'static str {
        "DELETE FROM image_tags WHERE image_hash = ? AND tag_name = ?"
    }

    fn delete_image_statement() -> &'static str {
        "DELETE FROM images WHERE hash = ?"
    }

    fn delete_tags_by_image_statement() -> &'static str {
        "DELETE FROM image_tags WHERE image_hash = ?"
    }

    fn exists_date_until_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_metadatas WHERE image_metadatas.image_hash = images.hash AND created_at <= {})",
            Self::placeholder(idx)
        )
    }

    fn exists_date_since_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_metadatas WHERE image_metadatas.image_hash = images.hash AND created_at >= {})",
            Self::placeholder(idx)
        )
    }

    fn migration() -> Vec<&'static str> {
        vec![
            r#"CREATE TABLE IF NOT EXISTS images (
                hash TEXT PRIMARY KEY,
                source TEXT
            );"#,
            r#"CREATE TABLE IF NOT EXISTS image_metadatas (
                image_hash TEXT,
                width INTEGER NOT NULL,
                height INTEGER NOT NULL,
                format TEXT NOT NULL,
                color_type TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (image_hash),
                FOREIGN KEY (image_hash) REFERENCES images(hash) ON DELETE CASCADE
            );"#,
            r#"CREATE TABLE IF NOT EXISTS tags (
                name TEXT PRIMARY KEY
            );"#,
            r#"CREATE TABLE IF NOT EXISTS image_tags (
                image_hash TEXT,
                tag_name TEXT,
                PRIMARY KEY (image_hash, tag_name),
                FOREIGN KEY (image_hash) REFERENCES images(hash) ON DELETE CASCADE,
                FOREIGN KEY (tag_name) REFERENCES tags(name) ON DELETE CASCADE
            );"#,
            r#"CREATE VIEW IF NOT EXISTS image_with_metadata
                AS SELECT * FROM images
                LEFT JOIN image_metadatas ON images.hash = image_metadatas.image_hash;"#,
        ]
    }

    fn update_source_statement() -> &'static str {
        "UPDATE images SET source = ? WHERE hash = ?"
    }
}
