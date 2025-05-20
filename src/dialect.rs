//! # SQL Dialect Module
//!
//! This module defines the `Dialect` trait, which abstracts over the differences in
//! SQL syntax and behavior across various database systems. The `Dialect` trait
//! provides methods for generating database-specific SQL statements and queries
//! to ensure compatibility with the target database, such as SQLite, PostgreSQL, or MySQL.
//!
//! The module includes a compile-time determination of the current SQL dialect used,
//! driven by feature flags. When the `sqlite` feature is enabled, the `CurrentDialect`
//! type alias is set to `sqlite::SqliteDialect`.
//!
//! ## Key Components
//! - **`Dialect` Trait**: Outlines methods for generating SQL statements and queries
//!   that are dialect-specific. This includes handling placeholders, conditional logic
//!   for inserts, and more.
//! - **`CurrentDialect` Alias**: Represents the SQL dialect used based on current feature flags,
//!   allowing higher-level code to interact with the database through a common interface.
//!
//! The goal of this module is to allow higher-level application logic to remain agnostic
//! to the underlying SQL dialect, making it simpler to add support for additional
//! databases in the future.

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
mod sqlite;

/// The current SQL dialect used at compile time, determined by feature flags.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type CurrentDialect = sqlite::SqliteDialect;

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type Db = sqlx::Sqlite;

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type CurrentRow = sqlx::sqlite::SqliteRow;

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
mod postgres;

/// The current SQL dialect used at compile time, determined by feature flags.
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type CurrentDialect = postgres::PostgresDialect;

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type Db = sqlx::Postgres;

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type CurrentRow = sqlx::postgres::PgRow;

fn qualify(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, table),
        None => table.to_string(),
    }
}

/// A trait for SQL dialects to support database-specific query generation.
///
/// This trait provides methods that return SQL strings compatible with the
/// target database (e.g., SQLite, PostgreSQL, MySQL). The goal is to abstract
/// away differences in placeholder syntax, conditional insert behavior, and
/// DELETE/SELECT semantics so that higher-level logic can remain dialect-agnostic.
pub trait Dialect {
    fn placeholder(idx: usize) -> String;

    fn exists_image(schema: Option<&str>) -> String {
        format!(
            "SELECT EXISTS ( SELECT 1 FROM {} WHERE hash = {} )",
            qualify(schema, "images"),
            Self::placeholder(1)
        )
    }

    fn exists_tag_query(schema: Option<&str>, idx: usize) -> String {
        let table = qualify(schema, "image_tags");
        format!(
            "EXISTS (SELECT 1 FROM {table} WHERE {table}.image_hash = image_with_metadata.hash AND {table}.tag_name = {})",
            Self::placeholder(idx)
        )
    }

    fn exists_date_until_query(schema: Option<&str>, idx: usize) -> String {
        let table = qualify(schema, "image_metadatas");
        format!(
            "EXISTS (SELECT 1 FROM {table} WHERE {table}.image_hash = images.hash AND created_at <= {})",
            Self::placeholder(idx)
        )
    }

    fn exists_date_since_query(schema: Option<&str>, idx: usize) -> String {
        let table = qualify(schema, "image_metadatas");
        format!(
            "EXISTS (SELECT 1 FROM {table} WHERE {table}.image_hash = images.hash AND created_at >= {})",
            Self::placeholder(idx)
        )
    }

    fn ensure_image_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT OR IGNORE INTO {} (hash) VALUES ({})",
            qualify(schema, "images"),
            Self::placeholder(1)
        )
    }

    fn ensure_tag_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT OR IGNORE INTO {} (name) VALUES ({})",
            qualify(schema, "tags"),
            Self::placeholder(1)
        )
    }

    fn ensure_metadata_statement(schema: Option<&str>) -> String {
        format!(
            r#"INSERT OR IGNORE INTO {}
            (image_hash, width, height, format, color_type, file_size, created_at, duration)
            VALUES ({}, {}, {}, {}, {}, {}, {}, {})"#,
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

    fn update_source_statement(schema: Option<&str>) -> String {
        format!(
            "UPDATE {} SET source = {} WHERE hash = {}",
            qualify(schema, "images"),
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }

    fn query_source_statement(schema: Option<&str>) -> String {
        format!(
            "SELECT source FROM {} WHERE hash = {}",
            qualify(schema, "images"),
            Self::placeholder(1)
        )
    }

    fn ensure_image_tag_statement(schema: Option<&str>) -> String {
        format!(
            "INSERT OR IGNORE INTO {} (image_hash, tag_name) VALUES ({}, {})",
            qualify(schema, "image_tags"),
            Self::placeholder(1),
            Self::placeholder(2),
        )
    }

    fn query_image_statement(schema: Option<&str>, condition: String) -> String {
        format!(
            "SELECT hash FROM {} {}",
            qualify(schema, "image_with_metadata"),
            condition
        )
    }

    fn count_image_statement(schema: Option<&str>, condition: String) -> String {
        format!(
            "SELECT COUNT(*) FROM {} {}",
            qualify(schema, "image_with_metadata"),
            condition
        )
    }

    fn count_image_by_tag_statement(schema: Option<&str>) -> String {
        format!(
            "SELECT count FROM {} WHERE tag_name = {}",
            qualify(schema, "tag_counts"),
            Self::placeholder(1)
        )
    }

    fn refresh_tag_counts_statement(schema: Option<&str>) -> Vec<String> {
        vec![
            format!("DELETE FROM {};", qualify(schema, "tag_counts")),
            format!(
                "INSERT INTO {} SELECT tag_name, COUNT(*) FROM {} GROUP BY tag_name;",
                qualify(schema, "tag_counts"),
                qualify(schema, "image_tags")
            ),
        ]
    }

    fn query_tag_statement(schema: Option<&str>, condition: String) -> String {
        format!("SELECT name FROM {} {}", qualify(schema, "tags"), condition)
    }

    fn query_tags_by_image_statement(schema: Option<&str>) -> String {
        format!(
            "SELECT tag_name FROM {} WHERE image_hash = {}",
            qualify(schema, "image_tags"),
            Self::placeholder(1)
        )
    }

    fn query_metadata_statement(schema: Option<&str>) -> String {
        format!(
            "SELECT * FROM {} WHERE image_hash = {}",
            qualify(schema, "image_metadatas"),
            Self::placeholder(1)
        )
    }

    fn delete_image_tag_statement(schema: Option<&str>) -> String {
        format!(
            "DELETE FROM {} WHERE image_hash = {} AND tag_name = {}",
            qualify(schema, "image_tags"),
            Self::placeholder(1),
            Self::placeholder(2),
        )
    }

    fn delete_image_statement(schema: Option<&str>) -> String {
        format!(
            "DELETE FROM {} WHERE hash = {}",
            qualify(schema, "images"),
            Self::placeholder(1)
        )
    }

    fn delete_tags_by_image_statement(schema: Option<&str>) -> String {
        format!(
            "DELETE FROM {} WHERE image_hash = {}",
            qualify(schema, "image_tags"),
            Self::placeholder(1)
        )
    }

    async fn migration(pool: &sqlx::Pool<Db>, schema: Option<&str>) -> Result<(), sqlx::Error>;
}
