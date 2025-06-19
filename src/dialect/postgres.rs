use super::Dialect;

/// Postgres dialect implementation of the `Dialect` trait.
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn placeholder(idx: usize) -> String {
        format!("${idx}")
    }

    fn ensure_image_statement() -> String {
        format!(
            "INSERT INTO images (hash) VALUES ({}) ON CONFLICT DO NOTHING",
            Self::placeholder(1)
        )
    }

    fn ensure_tag_statement() -> String {
        format!(
            "INSERT INTO tags (name) VALUES ({}) ON CONFLICT DO NOTHING",
            Self::placeholder(1)
        )
    }

    fn ensure_metadata_statement() -> String {
        format!(
            r#"INSERT INTO image_metadatas
            (image_hash, width, height, format, color_type, file_size, created_at, duration)
            VALUES ({}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT DO NOTHING"#,
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

    fn ensure_image_tag_statement() -> String {
        format!(
            "INSERT INTO image_tags (image_hash, tag_name) VALUES ({}, {}) ON CONFLICT DO NOTHING",
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }
}
