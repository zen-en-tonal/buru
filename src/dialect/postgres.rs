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

    async fn migration(pool: &sqlx::Pool<Db>, schema: Option<&str>) -> Result<(), sqlx::Error> {
        let mut stmts = vec![];

        if let Some(s) = schema {
            stmts.push(format!(r#"CREATE SCHEMA IF NOT EXISTS "{}";"#, s));
        }

        stmts.push(format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            hash TEXT PRIMARY KEY,
            source TEXT
        );
        "#,
            qualify(schema, "images")
        ));

        stmts.push(format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            image_hash TEXT PRIMARY KEY,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            format TEXT NOT NULL,
            color_type TEXT NOT NULL,
            file_size BIGINT NOT NULL,
            created_at TEXT NOT NULL,
            duration DOUBLE PRECISION,
            FOREIGN KEY (image_hash) REFERENCES {}(hash) ON DELETE CASCADE
        );
        "#,
            qualify(schema, "image_metadatas"),
            qualify(schema, "images")
        ));

        stmts.push(format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            name TEXT PRIMARY KEY
        );
        "#,
            qualify(schema, "tags")
        ));

        stmts.push(format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            image_hash TEXT,
            tag_name TEXT,
            PRIMARY KEY (image_hash, tag_name),
            FOREIGN KEY (image_hash) REFERENCES {}(hash) ON DELETE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES {}(name) ON DELETE CASCADE
        );
        "#,
            qualify(schema, "image_tags"),
            qualify(schema, "images"),
            qualify(schema, "tags")
        ));

        stmts.push(format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            tag_name TEXT PRIMARY KEY,
            count BIGINT NOT NULL,
            FOREIGN KEY (tag_name) REFERENCES {}(name) ON DELETE CASCADE
        );
        "#,
            qualify(schema, "tag_counts"),
            qualify(schema, "tags")
        ));

        let view_schema = schema_prefix(schema);
        stmts.push(format!(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_views WHERE schemaname = '{schema}' AND viewname = 'image_with_metadata'
                ) THEN
                    EXECUTE $v$
                        CREATE VIEW {view_schema}image_with_metadata AS
                        SELECT * FROM {view_schema}images
                        LEFT JOIN {view_schema}image_metadatas ON {view_schema}images.hash = {view_schema}image_metadatas.image_hash;
                    $v$;
                END IF;
            END
            $$;
            "#,
            schema = schema.unwrap_or("public"),
            view_schema = view_schema
        ));

        for stmt in stmts {
            sqlx::query(&stmt).execute(pool).await?;
        }

        Ok(())
    }
}

fn schema_prefix(schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("\"{}\".", s),
        None => "".to_string(),
    }
}
