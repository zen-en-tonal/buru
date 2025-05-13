use super::{Db, Dialect};
use sqlx::Row;

/// SQLite dialect implementation of the `Dialect` trait.
#[cfg(feature = "sqlite")]
pub struct SqliteDialect;

#[cfg(feature = "sqlite")]
impl Dialect for SqliteDialect {
    fn placeholder(_idx: usize) -> String {
        "?".to_string()
    }

    async fn migration(pool: &sqlx::Pool<Db>) -> Result<(), sqlx::Error> {
        let stmts = vec![
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
                created_at TEXT NOT NULL,
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
            r#"CREATE TABLE IF NOT EXISTS tag_counts (
                tag_name TEXT PRIMARY KEY,
                count INTEGER NOT NULL,
                FOREIGN KEY (tag_name) REFERENCES tags(name) ON DELETE CASCADE
            );"#,
        ];

        for stmt in stmts {
            sqlx::query(stmt).execute(pool).await?;
        }

        maybe_add_duration_column(pool).await?;

        Ok(())
    }
}

async fn maybe_add_duration_column(pool: &sqlx::Pool<Db>) -> Result<(), sqlx::Error> {
    let rows = sqlx::query("PRAGMA table_info(image_metadatas);")
        .fetch_all(pool)
        .await?;

    let has_duration = rows.iter().any(|row| {
        let name: &str = row.get("name");
        name == "duration"
    });

    if !has_duration {
        sqlx::query("ALTER TABLE image_metadatas ADD COLUMN duration REAL;")
            .execute(pool)
            .await?;
    }

    Ok(())
}
