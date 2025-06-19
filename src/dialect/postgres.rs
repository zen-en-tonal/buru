use super::{Db, Dialect};
use crate::dialect::qualify;

/// Postgres dialect implementation of the `Dialect` trait.
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn placeholder(idx: usize) -> String {
        format!("${idx}")
    }

    fn ensure_image_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT INTO {} (hash) VALUES ({}) ON CONFLICT DO NOTHING",
            qualify(schema, "images"),
            Self::placeholder(1)
        )
    }

    fn ensure_tag_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT INTO {} (name) VALUES ({}) ON CONFLICT DO NOTHING",
            qualify(schema, "tags"),
            Self::placeholder(1)
        )
    }

    fn ensure_metadata_statement(schema: Option<&str>) -> String {
        format!(
            r#"INSERT INTO {}
            (image_hash, width, height, format, color_type, file_size, created_at, duration)
            VALUES ({}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT DO NOTHING"#,
            qualify(schema, "image_metadatas"),
            Self::placeholder(1),
            Self::placeholder(2),
            Self::placeholder(3),
            Self::placeholder(4),
            Self::placeholder(5),
            Self::placeholder(6),
            Self::placeholder(7),
            Self::placeholder(8)
        )
    }

    fn ensure_image_tag_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT INTO {} (image_hash, tag_name) VALUES ({}, {}) ON CONFLICT DO NOTHING",
            qualify(schema, "image_tags"),
            Self::placeholder(1),
            Self::placeholder(2),
        )
        }
}
