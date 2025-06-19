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

/// A trait for SQL dialects to support database-specific query generation.
///
/// This trait provides methods that return SQL strings compatible with the
/// target database (e.g., SQLite, PostgreSQL, MySQL). The goal is to abstract
/// away differences in placeholder syntax, conditional insert behavior, and
/// DELETE/SELECT semantics so that higher-level logic can remain dialect-agnostic.
pub trait Dialect {
    fn placeholder(idx: usize) -> String;

    fn exists_image() -> String {
        format!(
            "SELECT EXISTS (SELECT 1 FROM images WHERE hash = {})",
            Self::placeholder(1)
        )
    }

    fn exists_tag_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_tags WHERE image_tags.image_hash = image_with_metadata.hash AND image_tags.tag_name = {})",
            Self::placeholder(idx)
        )
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

    fn ensure_image_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO images (hash) VALUES ({})",
            Self::placeholder(1)
        )
    }

    fn ensure_tag_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO tags (name) VALUES ({})",
            Self::placeholder(1)
        )
    }

    fn ensure_metadata_statement() -> String {
        format!(
            r#"INSERT OR IGNORE INTO image_metadatas
            (image_hash, width, height, format, color_type, file_size, created_at, duration)
            VALUES ({}, {}, {}, {}, {}, {}, {}, {})"#,
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

    fn update_source_statement() -> String {
        format!(
            "UPDATE images SET source = {} WHERE hash = {}",
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }

    fn query_source_statement() -> String {
        format!(
            "SELECT source FROM images WHERE hash = {}",
            Self::placeholder(1)
        )
    }

    fn ensure_image_tag_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO image_tags (image_hash, tag_name) VALUES ({}, {})",
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }

    fn query_image_statement(condition: String) -> String {
        format!("SELECT hash FROM image_with_metadata {}", condition)
    }

    fn count_image_statement(condition: String) -> String {
        format!("SELECT COUNT(*) FROM image_with_metadata {}", condition)
    }

    fn count_image_by_tag_statement() -> String {
        format!(
            "SELECT count FROM tag_counts WHERE tag_name = {}",
            Self::placeholder(1)
        )
    }

    fn refresh_tag_counts_statement() -> Vec<String> {
        vec![
            "DELETE FROM tag_counts;".to_string(),
            "INSERT INTO tag_counts SELECT tag_name, COUNT(*) FROM image_tags GROUP BY tag_name;"
                .to_string(),
        ]
    }

    fn query_tag_statement(condition: String) -> String {
        format!("SELECT name FROM tags {}", condition)
    }

    fn query_tags_by_image_statement() -> String {
        format!(
            "SELECT tag_name FROM image_tags WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }

    fn query_metadata_statement() -> String {
        format!(
            "SELECT * FROM image_metadatas WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }

    fn delete_image_tag_statement() -> String {
        format!(
            "DELETE FROM image_tags WHERE image_hash = {} AND tag_name = {}",
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }

    fn delete_image_statement() -> String {
        format!("DELETE FROM images WHERE hash = {}", Self::placeholder(1))
    }

    fn delete_tags_by_image_statement() -> String {
        format!(
            "DELETE FROM image_tags WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }
}
