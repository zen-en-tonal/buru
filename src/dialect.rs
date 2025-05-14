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

#[cfg(feature = "sqlite")]
mod sqlite;

/// The current SQL dialect used at compile time, determined by feature flags.
#[cfg(feature = "sqlite")]
pub type CurrentDialect = sqlite::SqliteDialect;

#[cfg(feature = "sqlite")]
pub type Db = sqlx::Sqlite;

#[cfg(feature = "sqlite")]
pub type CurrentRow = sqlx::sqlite::SqliteRow;

/// A trait for SQL dialects to support database-specific query generation.
///
/// This trait provides methods that return SQL strings compatible with the
/// target database (e.g., SQLite, PostgreSQL, MySQL). The goal is to abstract
/// away differences in placeholder syntax, conditional insert behavior, and
/// DELETE/SELECT semantics so that higher-level logic can remain dialect-agnostic.
pub trait Dialect {
    /// Returns the SQL placeholder syntax for the given parameter index.
    ///
    /// - SQLite: `?`
    /// - PostgreSQL: `$1`, `$2`, ...
    ///
    /// # Parameters
    /// - `idx`: The 1-based parameter index (used in dialects that number placeholders).
    fn placeholder(idx: usize) -> String;

    fn exists_image() -> String {
        format!(
            "SELECT EXISTS ( SELECT 1 FROM images WHERE hash = {} )",
            Self::placeholder(1)
        )
    }

    /// Returns a SQL `EXISTS` subquery to check if an image is tagged with a given tag.
    ///
    /// The returned SQL should be used within a WHERE clause and include a placeholder
    /// for the tag name.
    ///
    /// # Parameters
    /// - `idx`: The index to be used in the query for tagging.
    fn exists_tag_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_tags WHERE image_tags.image_hash = image_with_metadata.hash AND image_tags.tag_name = {})",
            Self::placeholder(idx)
        )
    }

    /// Returns a SQL query to check if a date is until a certain point.
    ///
    /// # Parameters
    /// - `idx`: The parameter index for the date in the query.
    fn exists_date_until_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_metadatas WHERE image_metadatas.image_hash = images.hash AND created_at <= {})",
            Self::placeholder(idx)
        )
    }

    /// Returns a SQL query to check if a date is since a certain point.
    ///
    /// # Parameters
    /// - `idx`: The parameter index for the date in the query.
    fn exists_date_since_query(idx: usize) -> String {
        format!(
            "EXISTS (SELECT 1 FROM image_metadatas WHERE image_metadatas.image_hash = images.hash AND created_at >= {})",
            Self::placeholder(idx)
        )
    }

    /// Returns the SQL statement to ensure an image exists in the `images` table.
    ///
    /// Usually implemented as an insert that ignores duplicates (e.g., `INSERT OR IGNORE`).
    fn ensure_image_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO images (hash) VALUES ({})",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement to ensure a tag exists in the `tags` table.
    ///
    /// Should insert the tag name only if it doesn't already exist.
    fn ensure_tag_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO tags (name) VALUES ({})",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement to ensure metadata is up to date.
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

    /// Returns the SQL statement to update a source.
    fn update_source_statement() -> String {
        format!(
            "UPDATE images SET source = {} WHERE hash = {}",
            Self::placeholder(1),
            Self::placeholder(2)
        )
    }

    /// Returns the SQL statement to query a source.
    fn query_source_statement() -> String {
        format!(
            "SELECT source FROM images WHERE hash = {}",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement to ensure a tag is attached to an image.
    ///
    /// Should insert a `(image_hash, tag_name)` pair into the `image_tags` table
    /// without duplicating existing entries.
    fn ensure_image_tag_statement() -> String {
        format!(
            "INSERT OR IGNORE INTO image_tags (image_hash, tag_name) VALUES ({}, {})",
            Self::placeholder(1),
            Self::placeholder(2),
        )
    }

    /// Returns a full SELECT statement for retrieving image hashes that match
    /// the given condition clause.
    ///
    /// # Parameters
    /// - `condition`: The SQL fragment (e.g., WHERE clause) generated by the query module.
    fn query_image_statement(condition: String) -> String {
        format!("SELECT hash FROM image_with_metadata {}", condition)
    }

    /// Returns the SQL statement to count images that match the given condition.
    ///
    /// # Parameters
    /// - `condition`: The SQL fragment for filtering images.
    fn count_image_statement(condition: String) -> String {
        format!("SELECT COUNT(hash) FROM image_with_metadata {}", condition)
    }

    /// Returns the SQL statement to count images associated with a specific tag.
    ///
    /// This query should count the number of images that are linked to a given tag,
    /// typically identified by the tag's name or identifier.
    fn count_image_by_tag_statement() -> String {
        format!(
            "SELECT count FROM tag_counts WHERE tag_name = {}",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement to refresh the counts of tags associated with images.
    ///
    /// This statement should update the count of times each tag is used across all images,
    /// ensuring that the tag count reflects the current state of the database.
    fn refresh_tag_counts_statement() -> &'static str {
        r#"BEGIN TRANSACTION;
        DELETE FROM tag_counts;
        INSERT INTO tag_counts SELECT tag_name, COUNT(*) FROM image_tags GROUP BY tag_name;
        COMMIT;"#
    }

    /// Returns the SQL statement to query tags based on a condition.
    ///
    /// # Parameters
    /// - `condition`: The SQL condition to filter tags.
    fn query_tag_statement(condition: String) -> String {
        format!("SELECT name FROM tags {}", condition)
    }

    /// Returns the SQL statement to retrieve all tags for a given image hash.
    ///
    /// Should return a single-column result (`tag_name`).
    fn query_tags_by_image_statement() -> String {
        format!(
            "SELECT tag_name FROM image_tags WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement for querying metadata.
    fn query_metadata_statement() -> String {
        format!(
            "SELECT * FROM image_metadatas WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }

    /// Returns the SQL statement to remove a tag from an image.
    ///
    /// This should delete a row from the `image_tags` table based on
    /// both `image_hash` and `tag_name`.
    fn delete_image_tag_statement() -> String {
        format!(
            "DELETE FROM image_tags WHERE image_hash = {} AND tag_name = {}",
            Self::placeholder(1),
            Self::placeholder(2),
        )
    }

    /// Returns the SQL statement to delete an image from the `images` table.
    ///
    /// Typically used in combination with a `DELETE` from `image_tags`.
    fn delete_image_statement() -> String {
        format!("DELETE FROM images WHERE hash = {}", Self::placeholder(1))
    }

    /// Returns the SQL statement to delete all tags associated with an image.
    ///
    /// This removes all rows from `image_tags` where `image_hash` matches.
    fn delete_tags_by_image_statement() -> String {
        format!(
            "DELETE FROM image_tags WHERE image_hash = {}",
            Self::placeholder(1)
        )
    }

    /// Returns a list of SQL migration statements needed for setting up the database.
    async fn migration(pool: &sqlx::Pool<Db>) -> Result<(), sqlx::Error>;
}
