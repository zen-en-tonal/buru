use crate::{
    dialect::{CurrentDialect, Dialect},
    query::Query,
    storage::Md5Hash,
};
use sqlx::Pool;
use thiserror::Error;

#[cfg(feature = "sqlite")]
pub type Db = sqlx::Sqlite;

/// A database abstraction for storing and querying image-tag relationships.
///
/// This struct wraps an SQLx connection pool and provides high-level methods
/// to ensure the existence of images and tags, relate them, and query them.
/// The implementation is SQL dialect agnostic and delegates syntax to `Dialect`.
#[derive(Debug, Clone)]
pub struct Database {
    pool: Pool<Db>,
}

impl Database {
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
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!("Retry loop should return before exceeding max_retries")
    }

    /// Ensures that an image is present in the `images` table.
    ///
    /// This will insert the image hash if it does not already exist.
    /// On failure (e.g., DB error), returns a `DatabaseError::QueryFailed`.
    pub async fn ensure_image(&self, hash: &Md5Hash) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::ensure_image_statement();

        self.retry(|| async {
            sqlx::query(stmt)
                .bind(hash.clone().to_string())
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertImage { hash: hash.clone() },
                    sql: stmt.to_string(),
                    params_string: hash.clone().to_string(),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Ensures that a tag is present in the `tags` table.
    ///
    /// This will insert the tag string if it does not already exist.
    /// Returns `DatabaseError::QueryFailed` if the query fails.
    pub async fn ensure_tag(&self, tag: &str) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::ensure_tag_statement();

        self.retry(|| async {
            sqlx::query(stmt)
                .bind(tag)
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertTag {
                        tag: tag.to_string(),
                    },
                    sql: stmt.to_string(),
                    params_string: tag.to_string(),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Ensures that the given image has the given tag.
    ///
    /// Internally, this calls [`Database::ensure_image`] and [`Database::ensure_tag`] first, and then
    /// inserts into `image_tags` to relate them.
    ///
    /// This method is idempotent and safe to call multiple times.
    pub async fn ensure_image_has_tag(
        &self,
        hash: &Md5Hash,
        tag: &str,
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;
        self.ensure_tag(tag).await?;

        let stmt = CurrentDialect::ensure_image_tag_statement();

        self.retry(|| async {
            sqlx::query(stmt)
                .bind(hash.clone().to_string())
                .bind(tag)
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertImageTag {
                        hash: hash.clone(),
                        tag: tag.to_string(),
                    },
                    sql: stmt.to_string(),
                    params_string: format!("{},{}", hash.to_string(), tag),
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    /// Performs a tag-based query on images using a [`Query`] expression tree.
    ///
    /// Returns a list of image hashes that match the query.
    /// Query construction is handled by the `Query` module.
    pub async fn find_by_query(&self, query: Query) -> Result<Vec<Md5Hash>, DatabaseError> {
        let (sql, params) = query.to_sql();
        let stmt = CurrentDialect::query_image_statement(sql);

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
                        params_string: params.join(","),
                        source: e,
                    })
            })
            .await?
            .into_iter()
            .filter_map(|s| Md5Hash::try_from(s).ok())
            .collect();

        Ok(hashes)
    }

    /// Returns a list of tags associated with the given image hash.
    ///
    /// If no tags exist, returns an empty vector.
    pub async fn get_tags(&self, hash: &Md5Hash) -> Result<Vec<String>, DatabaseError> {
        let stmt = CurrentDialect::query_tags_by_image_statement();

        let rows = self
            .retry(|| async {
                sqlx::query_scalar(&stmt)
                    .bind(hash.clone().to_string())
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryFailed {
                        operation: DbOperation::QueryImages,
                        sql: stmt.to_string(),
                        params_string: hash.to_string(),
                        source: e,
                    })
            })
            .await?;

        Ok(rows)
    }

    /// Ensures that a specific tag is removed from the image.
    ///
    /// This removes one (image_hash, tag) relation from `image_tags`.
    pub async fn ensure_tag_removed(&self, hash: &Md5Hash, tag: &str) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::delete_image_tag_statement();

        self.retry(|| async {
            sqlx::query(stmt)
                .bind(hash.clone().to_string())
                .bind(tag)
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::DeleteImageTag {
                        hash: hash.clone(),
                        tag: tag.to_string(),
                    },
                    sql: stmt.to_string(),
                    params_string: format!("{},{}", hash.to_string(), tag),
                    source: e,
                })
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
    pub async fn ensure_image_removed(&self, hash: &Md5Hash) -> Result<(), DatabaseError> {
        let stmt_tags = CurrentDialect::delete_tags_by_image_statement();
        let stmt_image = CurrentDialect::delete_image_statement();

        self.retry(|| async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| DatabaseError::TransactionFailed { source: e })?;

            sqlx::query(stmt_tags)
                .bind(hash.clone().to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::DeleteImageTags { hash: hash.clone() },
                    sql: stmt_tags.to_string(),
                    params_string: hash.to_string(),
                    source: e,
                })?;

            sqlx::query(stmt_image)
                .bind(hash.clone().to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::DeleteImage { hash: hash.clone() },
                    sql: stmt_image.to_string(),
                    params_string: hash.to_string(),
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
    #[error("Query failed during {operation:?}: sql={sql}, params=[{params_string}]")]
    QueryFailed {
        operation: DbOperation,
        sql: String,
        params_string: String,
        #[source]
        source: sqlx::Error,
    },

    /// A failure to begin or commit a transaction.
    #[error("Failed to operate transaction")]
    TransactionFailed {
        #[source]
        source: sqlx::Error,
    },
}

/// Enum representing the kind of database operation being performed,
/// used for attaching context to [`DatabaseError::QueryFailed`].
#[derive(Debug)]
pub enum DbOperation {
    /// INSERT INTO images
    InsertImage { hash: Md5Hash },
    /// INSERT INTO tags
    InsertTag { tag: String },
    /// INSERT INTO image_tags
    InsertImageTag { hash: Md5Hash, tag: String },
    /// DELETE FROM image_tags WHERE ...
    DeleteImageTag { hash: Md5Hash, tag: String },
    /// DELETE FROM images WHERE ...
    DeleteImage { hash: Md5Hash },
    /// DELETE FROM image_tags WHERE image_hash = ...
    DeleteImageTags { hash: Md5Hash },
    /// SELECT tag_name FROM image_tags WHERE image_hash = ...
    QueryTags { hash: Md5Hash },
    /// General image query using dynamic conditions
    QueryImages,
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
                params_string: _,
                sql: _,
                source,
                operation: _,
            } => is_retryable_kind(source),
            DatabaseError::TransactionFailed { source } => is_retryable_kind(source),
        }
    }
}
