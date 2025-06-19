//! Database abstraction for image and tag management.
//!
//! This module provides structures and functions to facilitate
//! image and tag storage and retrieval within the system.
//! It provides:
//!
//! - `Database`: A struct representing the database, implementing high-level
//!   operations like ensuring image existence, associating metadata, tags,
//!   and sources, as well as querying and modifying these associations.
//! - `DatabaseError`: An error type that captures errors during
//!   database operations with context to identify the failed operation.
//! - Support for SQLx, particularly with SQLite for executing migrations and
//!   querying the database.
//!
//! The implementation is designed to be SQL dialect agnostic and
//! leverages the `Dialect` trait, which encapsulates database-specific
//! query syntax.
//!
//! ## Usage
//!
//! To use this module, instantiate a `Database` with a pool, often created
//! from a configuration. Use either the provided high-level methods to
//! store, query, or manipulate image-related data or build custom queries
//! leveraging the provided infrastructure and error handling.

use crate::{
    dialect::{CurrentDialect, CurrentRow, Db, Dialect},
    query::{ImageQuery, TagQuery},
    storage::{ImageMetadata, PixelHash},
};
use chrono::{DateTime, Utc};
pub use sqlx::Pool;
use sqlx::{Execute, FromRow, Row};
use std::str::FromStr;
use thiserror::Error;

pub type Pool = sqlx::Pool<Db>;

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("migrations/sqlite");

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("migrations/postgres");

/// Run database migrations using the pool provided.
///
/// # Arguments
///
/// * `pool` - The connection pool to the database.
///
/// # Returns
///
/// This function returns a `Result` indicating success or failure during
/// the migration process.
pub async fn run_migration(pool: &sqlx::Pool<Db>) -> Result<(), sqlx::Error> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

impl FromRow<'_, CurrentRow> for ImageMetadata {
    fn from_row(row: &CurrentRow) -> Result<Self, sqlx::Error> {
        let width: i32 = row.try_get("width")?;
        let height: i32 = row.try_get("height")?;
        let format: String = row.try_get("format")?;
        let color_type: String = row.try_get("color_type")?;
        let file_size: i64 = row.try_get("file_size")?;
        let created_at: String = row.try_get("created_at")?;
        let created_at = DateTime::from_str(&created_at).expect("");
        let duration: Option<f64> = row.try_get("duration")?;

        Ok(ImageMetadata {
            width: width as u32,
            height: height as u32,
            format,
            color_type,
            file_size: file_size as u64,
            created_at: Some(created_at),
            duration,
        })
    }
}

/// A database abstraction for storing and querying image-tag relationships.
///
/// This struct wraps an SQLx connection pool and provides high-level methods
/// to ensure the existence of images and tags, relate them, and query them.
/// The implementation is SQL dialect agnostic and delegates syntax to `Dialect`.
#[derive(Debug, Clone)]
pub struct Database {
    pub pool: Pool<Db>,
    pub schema: Option<String>,
}

impl Database {
    pub fn new(pool: sqlx::Pool<Db>) -> Self {
        Self { pool, schema: None }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());

        self
    }

    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        run_migration(&self.pool, self.schema.as_deref()).await
    }

    async fn retry<F, Fut, T>(&self, mut op: F) -> Result<T, DatabaseError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, DatabaseError>>,
    {
        let max_retries = 3;
        for attempt in 0..max_retries {
            let result = op().await;
            match result {
                Ok(v) => return Ok(v),
                Err(ref e) if e.is_retryable() && attempt + 1 < max_retries => {
                    // backoff: simple fixed delay or exponential if needed
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!("Retry loop should return before exceeding max_retries")
    }

    /// Determines if an image exists in the database by its pixel hash.
    ///
    /// This method checks the existence of an image in the `images` table using the provided pixel hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - A reference to the `PixelHash` of the image to check.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `bool`:
    /// - `true` if the image exists in the database,
    /// - `false` if the image does not exist.
    ///
    /// On failure, it returns a `DatabaseError`.
    pub async fn image_exists(&self, hash: &PixelHash) -> Result<bool, DatabaseError> {
        let stmt = CurrentDialect::exists_image(self.schema.as_deref());

        let res = self
            .retry(|| async {
                let query = sqlx::query_scalar(&stmt).bind(hash.clone().to_string());
                let sql = query.sql();
                query
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: sql.to_string(),
                        source: e,
                    })
            })
            .await?;

        Ok(res)
    }

    /// Ensures that an image is present in the `images` table.
    ///
    /// This will insert the image hash if it does not already exist.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image to insert.
    ///
    /// # Returns
    ///
    /// This function returns a `Result` indicating success or failure.
    pub async fn ensure_image(&self, hash: &PixelHash) -> Result<(), DatabaseError> {
        if self.image_exists(hash).await? {
            return Ok(());
        }

        let stmt = CurrentDialect::ensure_image_statement(self.schema.as_deref());

        self.retry(|| async {
            let query = sqlx::query(&stmt).bind(hash.clone().to_string());
            let sql = query.sql();
            query
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertImage { hash: hash.clone() },
                    sql: sql.to_string(),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Ensures that an image has associated metadata in the database.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    /// * `metadata` - The metadata attributes of the image.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of ensuring the metadata.
    pub async fn ensure_image_has_metadata(
        &self,
        hash: &PixelHash,
        metadata: &ImageMetadata,
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;

        let stmt = CurrentDialect::ensure_metadata_statement(self.schema.as_deref());

        self.retry(|| async {
            let query = sqlx::query(&stmt)
                .bind(hash.clone().to_string())
                .bind(metadata.width as i64)
                .bind(metadata.height as i64)
                .bind(&metadata.format)
                .bind(&metadata.color_type)
                .bind(metadata.file_size as i64)
                .bind(metadata.created_at.unwrap_or(Utc::now()).to_rfc3339())
                .bind(metadata.duration);
            let sql = query.sql();
            query
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertMetadata {
                        metadata: metadata.clone(),
                    },
                    sql: sql.to_string(),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Ensures that a set of tags is present in the `tags` table.
    ///
    /// # Arguments
    ///
    /// * `tags` - A slice of tag strings to ensure existence in the database.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn ensure_tags(&self, tags: &[&str]) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::ensure_tag_statement(self.schema.as_deref());

        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            for tag in tags.iter() {
                let query = sqlx::query(&stmt).bind(tag);
                let sql = query.sql();
                query
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::InsertTag {
                            tag: tag.to_string(),
                        },
                        sql: sql.to_string(),
                        source: e,
                    })?;
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })
        })
        .await?;
        Ok(())
    }

    /// Ensures that an image is associated with given tags.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    /// * `tags` - A slice of tag strings to associate with the image.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn ensure_image_has_tags(
        &self,
        hash: &PixelHash,
        tags: &[&str],
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;
        self.ensure_tags(tags).await?;

        let stmt = CurrentDialect::ensure_image_tag_statement(self.schema.as_deref());

        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            for tag in tags.iter() {
                let query = sqlx::query(&stmt).bind(hash.to_string()).bind(tag);
                let sql = query.sql();
                query
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::InsertImageTag {
                            hash: hash.clone(),
                            tag: tag.to_string(),
                        },
                        sql: sql.to_string(),
                        source: e,
                    })?;
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })
        })
        .await?;

        Ok(())
    }

    /// Ensures that an image is associated with a source string.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    /// * `source` - The source string to associate with the image.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn ensure_image_has_source(
        &self,
        hash: &PixelHash,
        source: &str,
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;

        let stmt = CurrentDialect::update_source_statement(self.schema.as_deref());

        self.retry(|| async {
            let query = sqlx::query(&stmt)
                .bind(source)
                .bind(hash.clone().to_string());
            let sql = query.sql();

            query
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::UpdateImageSource {
                        hash: hash.clone(),
                        source: source.to_string(),
                    },
                    sql: sql.to_string(),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Performs a tag-based query on images using an expression tree.
    ///
    /// # Arguments
    ///
    /// * `query` - The query expression representing the image search criteria.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of image hashes that match the query.
    pub async fn query_image(&self, query: ImageQuery) -> Result<Vec<PixelHash>, DatabaseError> {
        let (sql, params) = query.to_sql(self.schema.as_deref());
        let stmt = CurrentDialect::query_image_statement(self.schema.as_deref(), sql);

        let hashes = self
            .retry(|| async {
                let mut q = sqlx::query_scalar::<_, String>(&stmt);

                for param in &params {
                    q = q.bind(param);
                }

                q.fetch_all(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        source: e,
                    })
            })
            .await?
            .into_iter()
            .filter_map(|s| PixelHash::try_from(s).ok())
            .collect();

        Ok(hashes)
    }

    /// Performs a count of images that match a given query expression.
    ///
    /// # Arguments
    ///
    /// * `query` - The query expression representing the image search criteria.
    ///
    /// # Returns
    ///
    /// A `Result` containing the count of images that match the query.
    pub async fn count_image(&self, query: ImageQuery) -> Result<u64, DatabaseError> {
        let (sql, params) = query.to_sql(self.schema.as_deref());
        let stmt = CurrentDialect::count_image_statement(self.schema.as_deref(), sql);

        let count = self
            .retry(|| async {
                let mut q = sqlx::query_scalar(&stmt);

                for param in &params {
                    q = q.bind(param);
                }

                // cast into signed because some DBs do not support unsigned types.
                let count: i64 =
                    q.fetch_one(&self.pool)
                        .await
                        .map_err(|e| DatabaseError::QueryFailed {
                            operation: DbOperation::QueryImages,
                            sql: stmt.to_string(),
                            source: e,
                        })?;

                Ok(count as u64)
            })
            .await?;

        Ok(count)
    }

    /// Counts the number of images associated with a given tag.
    ///
    /// This method queries the database to find how many images are related
    /// to the specified tag. It provides a simple way to retrieve statistics
    /// about image-tag associations within the database.
    ///
    /// # Arguments
    ///
    /// * `tag` - A string slice that holds the tag for which the image count
    ///           is to be determined.
    ///
    /// # Returns
    ///
    /// This function returns a `Result` containing a `u64` representing the
    /// count of images associated with the given tag. If an error occurs
    /// during the query execution, the `Result` will contain a `DatabaseError`.
    pub async fn count_image_by_tag(&self, tag: &str) -> Result<u64, DatabaseError> {
        let stmt = CurrentDialect::count_image_by_tag_statement(self.schema.as_deref());

        let count = self
            .retry(|| async {
                let q = sqlx::query_scalar(&stmt).bind(tag);

                let count: i64 = q
                    .fetch_optional(&self.pool)
                    .await
                    .map(|r| r.unwrap_or_default())
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        source: e,
                    })?;

                Ok(count as u64)
            })
            .await?;

        Ok(count)
    }

    /// Refreshes the count of images associated with each tag in the database.
    ///
    /// This method recalculates the number of images associated with each tag and updates
    /// the database to reflect the current counts. It's useful for maintaining accurate statistics
    /// on image-tag associations after operations that change these relationships, such as adding
    /// or removing tags from images.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the operation.
    ///
    /// On success, it returns `Ok(())`. On failure, it returns a `DatabaseError` with context
    /// about the failed operation.
    pub async fn refresh_image_count(&self) -> Result<(), DatabaseError> {
        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            for stmt in CurrentDialect::refresh_tag_counts_statement(self.schema.as_deref()) {
                let q = sqlx::query(&stmt);

                q.execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        source: e,
                    })?;
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })
        })
        .await?;

        Ok(())
    }

    /// Performs a query on tags using a query expression tree.
    ///
    /// # Arguments
    ///
    /// * `query` - The query expression representing the tag search criteria.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of tag strings that match the query.
    pub async fn query_tags(&self, query: TagQuery) -> Result<Vec<String>, DatabaseError> {
        let (sql, params) = query.to_sql();
        let stmt = CurrentDialect::query_tag_statement(self.schema.as_deref(), sql);

        let hashes = self
            .retry(|| async {
                let mut q = sqlx::query_scalar::<_, String>(&stmt);

                for param in &params {
                    q = q.bind(param);
                }

                q.fetch_all(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryTags,
                        sql: stmt.to_string(),
                        source: e,
                    })
            })
            .await?
            .into_iter()
            .collect();

        Ok(hashes)
    }

    /// Returns a list of tags associated with the given image hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image to lookup.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of tag strings associated with the image.
    pub async fn get_tags(&self, hash: &PixelHash) -> Result<Vec<String>, DatabaseError> {
        let stmt = CurrentDialect::query_tags_by_image_statement(self.schema.as_deref());

        let rows = self
            .retry(|| async {
                sqlx::query_scalar(&stmt)
                    .bind(hash.clone().to_string())
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        source: e,
                    })
            })
            .await?;

        Ok(rows)
    }

    /// Retrieves metadata for a given image hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` of `ImageMetadata`.
    /// The `Option` will be `None` if metadata is not found.
    pub async fn get_metadata(
        &self,
        hash: &PixelHash,
    ) -> Result<Option<ImageMetadata>, DatabaseError> {
        let stmt = CurrentDialect::query_metadata_statement(self.schema.as_deref());

        let metadata: Option<ImageMetadata> = self
            .retry(|| async {
                sqlx::query_as(&stmt)
                    .bind(hash.clone().to_string())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        source: e,
                    })
            })
            .await?;

        Ok(metadata)
    }

    /// Retrieves the source information for a given image hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` of the source string.
    /// The `Option` will be `None` if the source is not found.
    pub async fn get_source(&self, hash: &PixelHash) -> Result<Option<String>, DatabaseError> {
        let stmt = CurrentDialect::query_source_statement(self.schema.as_deref());

        let soruce: Option<String> = self
            .retry(|| async {
                let query = sqlx::query_scalar(&stmt).bind(hash.clone().to_string());
                let sql = query.sql();

                query
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: sql.to_string(),
                        source: e,
                    })
            })
            .await?;

        Ok(soruce)
    }

    /// Ensures that specific tags are removed from the image.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image.
    /// * `tags` - A slice of tag strings to remove from the image.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn ensure_tags_removed(
        &self,
        hash: &PixelHash,
        tags: &[&str],
    ) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::delete_image_tag_statement(self.schema.as_deref());

        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            for tag in tags.iter() {
                let query = sqlx::query(&stmt).bind(hash.to_string()).bind(tag);
                let sql = query.sql();
                query
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::DeleteImageTag {
                            hash: hash.clone(),
                            tag: tag.to_string(),
                        },
                        sql: sql.to_string(),
                        source: e,
                    })?;
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })
        })
        .await?;

        Ok(())
    }

    /// Ensures that an image and all its tag relations are removed.
    ///
    /// This is a transactional operation that:
    /// 1. Deletes all related rows in `image_tags`
    /// 2. Deletes the image row in `images`
    ///
    /// If any step fails, the entire transaction is rolled back.
    ///
    /// # Arguments
    ///
    /// * `hash` - The pixel hash of the image to be removed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn ensure_image_removed(&self, hash: &PixelHash) -> Result<(), DatabaseError> {
        let stmt_tags = CurrentDialect::delete_tags_by_image_statement(self.schema.as_deref());
        let stmt_image = CurrentDialect::delete_image_statement(self.schema.as_deref());

        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            sqlx::query(&stmt_tags)
                .bind(hash.clone().to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::DeleteImageTags { hash: hash.clone() },
                    sql: stmt_tags.to_string(),
                    source: e,
                })?;

            sqlx::query(&stmt_image)
                .bind(hash.clone().to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::DeleteImage { hash: hash.clone() },
                    sql: stmt_image.to_string(),
                    source: e,
                })?;

            tx.commit()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })
        })
        .await?;

        Ok(())
    }
}

/// Represents errors that can occur during database operations.
///
/// Each variant includes contextual information to assist with debugging and error handling.
#[derive(Debug, Error)]
pub enum DatabaseError {
    /// A general SQL query failure, with full context including operation, SQL and parameters.
    #[error("Query failed during {operation:?}: sql={sql} source={source}")]
    QueryFailed {
        operation: DbOperation,
        sql: String,
        #[source]
        source: sqlx::Error,
    },

    /// A failure to begin or commit a transaction.
    #[error("Failed to operate transaction: source={source}")]
    TransactionFailed {
        #[source]
        source: sqlx::Error,
    },
}

/// Enum representing the kind of database operation being performed.
///
/// This enum is primarily used for attaching context to
/// [`DatabaseError::QueryFailed`], enabling more detailed error messages
/// that describe the specific operation that triggered an error.
#[derive(Debug)]
pub enum DbOperation {
    /// Operation for inserting a new entry into the `images` table.
    InsertImage {
        /// The hash of the image to be inserted, serving as a unique identifier.
        hash: PixelHash,
    },
    /// Operation for inserting a new entry into the `tags` table.
    InsertTag {
        /// The tag string to be inserted into the database.
        tag: String,
    },
    /// Operation for inserting a new entry into the `image_tags` table,
    /// which associates images with tags.
    InsertImageTag {
        /// The hash of the image to associate with the tag.
        hash: PixelHash,
        /// The tag string to associate with the image.
        tag: String,
    },
    /// Operation for deleting a specific tag association from the `image_tags` table.
    DeleteImageTag {
        /// The hash of the image from which to remove the tag.
        hash: PixelHash,
        /// The tag string to be removed from the image.
        tag: String,
    },
    /// Operation for deleting an image entry from the `images` table.
    DeleteImage {
        /// The hash of the image to be deleted, serving as a unique identifier.
        hash: PixelHash,
    },
    /// Operation for deleting all tag associations for a given image
    /// from the `image_tags` table.
    DeleteImageTags {
        /// The hash of the image for which all tags are to be removed.
        hash: PixelHash,
    },
    /// Operation for querying tags associated with a specific image hash
    /// from the `image_tags` table.
    QueryImageTags {
        /// The hash of the image whose associated tags are to be queried.
        hash: PixelHash,
    },
    /// General operation for querying images using complex, dynamic conditions
    /// specified by the user.
    QueryImages,
    /// Operation for inserting metadata associated with an image into the database.
    InsertMetadata {
        /// The `ImageMetadata` struct containing details about the image.
        metadata: ImageMetadata,
    },
    /// Operation for updating the source information of an image
    /// in the `images` table.
    UpdateImageSource {
        /// The hash of the image whose source information is to be updated.
        hash: PixelHash,
        /// The new source string to associate with the image.
        source: String,
    },
    /// Operation for querying tags from the `tags` table.
    QueryTags,
}

impl DatabaseError {
    fn is_retryable(&self) -> bool {
        let is_retryable_kind = |e: &sqlx::Error| {
            matches!(e, sqlx::Error::Io(_))
                || matches!(e, sqlx::Error::Protocol(_))
                || matches!(e, sqlx::Error::PoolTimedOut)
        };

        match self {
            DatabaseError::QueryFailed {
                sql: _,
                source,
                operation: _,
            } => is_retryable_kind(source),
            DatabaseError::TransactionFailed { source } => is_retryable_kind(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        database::{Database, Pool},
        dialect::Db,
        query::{ImageQuery, ImageQueryExpr, ImageQueryKind, TagQuery, TagQueryExpr, TagQueryKind},
        storage::{ImageMetadata, PixelHash},
    };
    use chrono::DateTime;
    use std::str::FromStr;

    #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
    async fn drop_schema(pool: sqlx::Pool<Db>, schema: Option<&str>) -> Result<(), sqlx::Error> {
        if let Some(schema) = schema {
            sqlx::query(&format!("DROP SCHEMA {} CASCADE", schema))
                .execute(&pool)
                .await?;
        }

        Ok(())
    }

    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    async fn drop_schema(_pool: sqlx::Pool<Db>, _schema: Option<&str>) -> Result<(), sqlx::Error> {
        Ok(())
    }

    #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
    async fn get_db() -> Database {
        use std::env;
        let conn =
            env::var("DATABASE_URL").unwrap_or("postgres://postgres:password@db/devdb".to_string());

        let pool = Pool::connect(&conn).await.unwrap();
        let schema = format!("test_{}", uuid::Uuid::new_v4().to_string().replace("-", ""));

        let db = Database::new(pool).with_schema(&schema);
        db.migrate().await.unwrap();

        db
    }

    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    async fn get_db() -> Database {
        let conn = ":memory:";

        let pool = Pool::connect(conn).await.unwrap();

        let db = Database::new(pool);
        db.migrate().await.unwrap();

        db
    }

    /// Verifies that `Database::with_migration` can be called multiple times
    /// on the same pool without error.
    ///
    /// This confirms that migrations are idempotent — i.e., calling them again
    /// does not fail or break schema assumptions.
    #[tokio::test]
    async fn test_migration_idempotency() {
        let db = get_db().await;

        db.migrate().await.unwrap();
        db.migrate().await.unwrap();

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Ensures that the same image can be inserted multiple times without causing an error.
    ///
    /// This function tests both the success of the insertion and idempotency, confirming
    /// that `ensure_image` executes successfully even if the image already exists in the database.
    #[tokio::test]
    async fn test_ensure_image() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        db.ensure_image(&image).await.unwrap();
        db.ensure_image(&image).await.unwrap();

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Ensures that an image can have an associated source and that it can be correctly retrieved.
    ///
    /// This test confirms the functionality of associating a source string with an image and
    /// ensures that this data can be accurately retrieved afterwards.
    #[tokio::test]
    async fn test_ensure_source() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        db.ensure_image_has_source(&image, "src").await.unwrap();

        assert_eq!(
            Some("src".to_string()),
            db.get_source(&image).await.unwrap()
        );

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Ensures that inserting the same metadata multiple times does not result in an error.
    ///
    /// This test validates both the success of metadata insertion and idempotency,
    /// confirming that `ensure_image_has_metadata` can be executed on existing metadata without errors.
    #[tokio::test]
    async fn test_ensure_metadata() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();
        let metadata = ImageMetadata {
            width: 200,
            height: 200,
            format: "image/png".to_string(),
            color_type: "rgba".to_string(),
            file_size: 1337,
            created_at: Some(DateTime::from_str("2025-05-02T01:18:49.678809123Z").unwrap()),
            duration: Some(1.0),
        };

        db.ensure_image_has_metadata(&image, &metadata)
            .await
            .unwrap();
        db.ensure_image_has_metadata(&image, &metadata)
            .await
            .unwrap();

        assert_eq!(Some(metadata), db.get_metadata(&image).await.unwrap());

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Ensures that metadata can be inserted and retrieved correctly without a `created_at` value.
    ///
    /// This test confirms that `ensure_image_has_metadata` correctly handles metadata entries
    /// that lack a `created_at` field.
    #[tokio::test]
    async fn test_ensure_metadata_without_created_at() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();
        let metadata = ImageMetadata {
            width: 200,
            height: 200,
            format: "image/png".to_string(),
            color_type: "rgba".to_string(),
            file_size: 1337,
            created_at: None,
            duration: None,
        };
        db.ensure_image_has_metadata(&image, &metadata)
            .await
            .unwrap();
        assert!(db.get_metadata(&image).await.unwrap().is_some());

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Performs a comprehensive test of image tag operations including:
    /// - Adding tags to an image
    /// - Preventing duplicate tags
    /// - Removing tags safely and idempotently
    /// - Verifying the final list of tags.
    #[tokio::test]
    async fn test_operate_image_tag() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        // Add tags "cat" and "dog" (including duplicate insertions)
        assert!(db.ensure_image_has_tags(&image, &["cat"]).await.is_ok());
        assert!(db.ensure_image_has_tags(&image, &["cat"]).await.is_ok());
        assert!(db.ensure_image_has_tags(&image, &["dog"]).await.is_ok());

        // Confirm both tags are present
        assert_eq!(
            vec!["cat".to_string(), "dog".to_string()],
            db.get_tags(&image).await.unwrap()
        );

        // Remove "dog" tag twice (should be safe and idempotent)
        db.ensure_tags_removed(&image, &["dog"]).await.unwrap();
        db.ensure_tags_removed(&image, &["dog"]).await.unwrap();

        // Confirm only "cat" remains
        assert_eq!(vec!["cat".to_string()], db.get_tags(&image).await.unwrap());

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Tests image querying based on tags, verifying that images are returned
    /// according to the specified criteria.
    ///
    /// This ensures that the query results match the expected images for "cat",
    /// "dog", and "cat and dog" parameters.
    #[tokio::test]
    async fn test_query_image() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image_cat = PixelHash::try_from("329435e5e66be809").unwrap();
        let image_dog = PixelHash::try_from("229435e5e66be809").unwrap();
        let image_cat_and_dog = PixelHash::try_from("129435e5e66be809").unwrap();

        assert!(db.ensure_image_has_tags(&image_cat, &["cat"]).await.is_ok());
        assert!(db.ensure_image_has_tags(&image_dog, &["dog"]).await.is_ok());
        assert!(
            db.ensure_image_has_tags(&image_cat_and_dog, &["cat"])
                .await
                .is_ok()
        );
        assert!(
            db.ensure_image_has_tags(&image_cat_and_dog, &["dog"])
                .await
                .is_ok()
        );

        let query_cat = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("cat")));
        let query_dog = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("dog")));
        let query_cat_and_dog = ImageQuery::new(ImageQueryKind::Where(
            ImageQueryExpr::tag("cat").and(ImageQueryExpr::tag("dog")),
        ));

        let mut res = db.query_image(query_cat).await.unwrap();
        res.sort();
        assert_eq!(vec![image_cat_and_dog.clone(), image_cat.clone()], res);

        let mut res = db.query_image(query_dog).await.unwrap();
        res.sort();
        assert_eq!(vec![image_cat_and_dog.clone(), image_dog.clone(),], res);

        let mut res = db.query_image(query_cat_and_dog).await.unwrap();
        res.sort();
        assert_eq!(vec![image_cat_and_dog], res);

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Tests the image counting functionality based on specific query criteria,
    /// ensuring correctness of count results.
    ///
    /// This test confirms that the counted results match the expected count for
    /// images associated with "cat", "dog", and both "cat and dog" tags.
    #[tokio::test]
    async fn test_count_image() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image_cat = PixelHash::try_from("329435e5e66be809").unwrap();
        let image_dog = PixelHash::try_from("229435e5e66be809").unwrap();
        let image_cat_and_dog = PixelHash::try_from("129435e5e66be809").unwrap();

        assert!(db.ensure_image_has_tags(&image_cat, &["cat"]).await.is_ok());
        assert!(db.ensure_image_has_tags(&image_dog, &["dog"]).await.is_ok());
        assert!(
            db.ensure_image_has_tags(&image_cat_and_dog, &["cat", "dog"])
                .await
                .is_ok()
        );

        let query_cat = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("cat")));
        let query_dog = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("dog")));
        let query_cat_and_dog = ImageQuery::new(ImageQueryKind::Where(
            ImageQueryExpr::tag("cat").and(ImageQueryExpr::tag("dog")),
        ));

        assert_eq!(2, db.count_image(query_cat).await.unwrap());
        assert_eq!(2, db.count_image(query_dog).await.unwrap());
        assert_eq!(1, db.count_image(query_cat_and_dog).await.unwrap());

        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    #[tokio::test]
    async fn test_count_image_by_tag() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        let image_cat = PixelHash::try_from("329435e5e66be809").unwrap();
        let image_dog = PixelHash::try_from("229435e5e66be809").unwrap();
        let image_cat_and_dog = PixelHash::try_from("129435e5e66be809").unwrap();

        assert!(db.ensure_image_has_tags(&image_cat, &["cat"]).await.is_ok());
        assert!(db.ensure_image_has_tags(&image_dog, &["dog"]).await.is_ok());
        assert!(
            db.ensure_image_has_tags(&image_cat_and_dog, &["cat", "dog"])
                .await
                .is_ok()
        );

        db.refresh_image_count().await.unwrap();

        assert_eq!(2, db.count_image_by_tag("cat").await.unwrap());
        assert_eq!(2, db.count_image_by_tag("dog").await.unwrap());
        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }

    /// Tests the querying of tags ensuring they can be accurately retrieved based on different query types.
    ///
    /// This confirms the correct behavior for exact match, containment, and retrieval of all tag entries.
    #[tokio::test]
    async fn test_query_tags() {
        let db = get_db().await;
        db.migrate().await.unwrap();

        assert!(db.ensure_tags(&["cat"]).await.is_ok());
        assert!(db.ensure_tags(&["dog"]).await.is_ok());

        let query_cat = TagQuery::new(TagQueryKind::Where(TagQueryExpr::Exact("cat".to_string())));
        let query_dog = TagQuery::new(TagQueryKind::Where(TagQueryExpr::Exact("dog".to_string())));
        let query_all = TagQuery::new(TagQueryKind::All);
        let query_contains_ca = TagQuery::new(TagQueryKind::Where(TagQueryExpr::Contains(
            "ca".to_string(),
        )));

        assert_eq!(
            vec!["cat".to_string(), "dog".to_string()],
            db.query_tags(query_all).await.unwrap()
        );
        assert_eq!(
            vec!["cat".to_string()],
            db.query_tags(query_cat).await.unwrap()
        );
        assert_eq!(
            vec!["dog".to_string()],
            db.query_tags(query_dog).await.unwrap()
        );
        assert_eq!(
            vec!["cat".to_string()],
            db.query_tags(query_contains_ca).await.unwrap()
        );
        drop_schema(db.pool, db.schema.as_deref()).await.unwrap();
    }
}
