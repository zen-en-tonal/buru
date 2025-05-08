use crate::{
    dialect::{CurrentDialect, Dialect},
    query::{ImageQuery, TagQuery},
    storage::{ImageMetadata, PixelHash},
};
use chrono::{DateTime, Utc};
pub use sqlx::Pool;
use sqlx::{Execute, FromRow, Row};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "sqlite")]
pub type Db = sqlx::Sqlite;

type CurrentRow = sqlx::sqlite::SqliteRow;

pub async fn run_migration(pool: &sqlx::Pool<Db>) -> Result<(), sqlx::Error> {
    for stmt in CurrentDialect::migration() {
        sqlx::query(stmt).execute(pool).await?;
    }

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

        Ok(ImageMetadata {
            width: width as u32,
            height: height as u32,
            format,
            color_type,
            file_size: file_size as u64,
            created_at: Some(created_at),
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
    pool: Pool<Db>,
}

impl Database {
    pub async fn with_migration(pool: sqlx::Pool<Db>) -> Result<Self, sqlx::Error> {
        run_migration(&pool).await?;

        Ok(Self { pool })
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
    pub async fn ensure_image(&self, hash: &PixelHash) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::ensure_image_statement();

        self.retry(|| async {
            let query = sqlx::query(stmt).bind(hash.clone().to_string());
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

    pub async fn ensure_image_has_metadata(
        &self,
        hash: &PixelHash,
        metadata: &ImageMetadata,
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;

        self.retry(|| async {
            let query = sqlx::query(CurrentDialect::ensure_metadata_statement())
                .bind(hash.clone().to_string())
                .bind(metadata.width as i64)
                .bind(metadata.height as i64)
                .bind(&metadata.format)
                .bind(&metadata.color_type)
                .bind(metadata.file_size as i64)
                .bind(metadata.created_at.unwrap_or(Utc::now()).to_rfc3339());
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

    /// Ensures that a tag is present in the `tags` table.
    ///
    /// This will insert the tag string if it does not already exist.
    /// Returns `DatabaseError::QueryFailed` if the query fails.
    pub async fn ensure_tag(&self, tag: &str) -> Result<(), DatabaseError> {
        let stmt = CurrentDialect::ensure_tag_statement();

        self.retry(|| async {
            let query = sqlx::query(stmt).bind(tag);
            let sql = query.sql();
            query
                .execute(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryFailed {
                    operation: DbOperation::InsertTag {
                        tag: tag.to_string(),
                    },
                    sql: sql.to_string(),
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
        hash: &PixelHash,
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
                    source: e,
                })
        })
        .await?;

        Ok(())
    }

    pub async fn ensure_image_has_source(
        &self,
        hash: &PixelHash,
        source: &str,
    ) -> Result<(), DatabaseError> {
        self.ensure_image(hash).await?;

        self.retry(|| async {
            let query = sqlx::query(CurrentDialect::update_source_statement())
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

    /// Performs a tag-based query on images using a [`Query`] expression tree.
    ///
    /// Returns a list of image hashes that match the query.
    /// Query construction is handled by the `Query` module.
    pub async fn query_image(&self, query: ImageQuery) -> Result<Vec<PixelHash>, DatabaseError> {
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
                        source: e,
                    })
            })
            .await?
            .into_iter()
            .filter_map(|s| PixelHash::try_from(s).ok())
            .collect();

        Ok(hashes)
    }

    pub async fn query_tags(&self, query: TagQuery) -> Result<Vec<String>, DatabaseError> {
        let (sql, params) = query.to_sql();
        let stmt = CurrentDialect::query_tag_statement(sql);

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
    /// If no tags exist, returns an empty vector.
    pub async fn get_tags(&self, hash: &PixelHash) -> Result<Vec<String>, DatabaseError> {
        let stmt = CurrentDialect::query_tags_by_image_statement();

        let rows = self
            .retry(|| async {
                sqlx::query_scalar(stmt)
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

    pub async fn get_metadata(
        &self,
        hash: &PixelHash,
    ) -> Result<Option<ImageMetadata>, DatabaseError> {
        let stmt = CurrentDialect::query_metadata_statement();

        let metadata: Option<ImageMetadata> = self
            .retry(|| async {
                sqlx::query_as(stmt)
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

    pub async fn get_source(&self, hash: &PixelHash) -> Result<Option<String>, DatabaseError> {
        let soruce: Option<String> = self
            .retry(|| async {
                let query = sqlx::query_scalar(CurrentDialect::query_source_statement())
                    .bind(hash.clone().to_string());
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

    /// Ensures that a specific tag is removed from the image.
    ///
    /// This removes one (image_hash, tag) relation from `image_tags`.
    pub async fn ensure_tag_removed(
        &self,
        hash: &PixelHash,
        tag: &str,
    ) -> Result<(), DatabaseError> {
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
    pub async fn ensure_image_removed(&self, hash: &PixelHash) -> Result<(), DatabaseError> {
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
                    source: e,
                })?;

            sqlx::query(stmt_image)
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
    #[error("Query failed during {operation:?}: sql={sql}")]
    QueryFailed {
        operation: DbOperation,
        sql: String,
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
    InsertImage {
        hash: PixelHash,
    },
    /// INSERT INTO tags
    InsertTag {
        tag: String,
    },
    /// INSERT INTO image_tags
    InsertImageTag {
        hash: PixelHash,
        tag: String,
    },
    /// DELETE FROM image_tags WHERE ...
    DeleteImageTag {
        hash: PixelHash,
        tag: String,
    },
    /// DELETE FROM images WHERE ...
    DeleteImage {
        hash: PixelHash,
    },
    /// DELETE FROM image_tags WHERE image_hash = ...
    DeleteImageTags {
        hash: PixelHash,
    },
    /// SELECT tag_name FROM image_tags WHERE image_hash = ...
    QueryImageTags {
        hash: PixelHash,
    },
    /// General image query using dynamic conditions
    QueryImages,
    InsertMetadata {
        metadata: ImageMetadata,
    },
    UpdateImageSource {
        hash: PixelHash,
        source: String,
    },
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
        database::{Database, Db, Pool},
        query::{ImageQuery, ImageQueryExpr, ImageQueryKind, TagQuery, TagQueryExpr, TagQueryKind},
        storage::{ImageMetadata, PixelHash},
    };
    use chrono::DateTime;
    use std::str::FromStr;

    /// Returns an in-memory SQLite connection pool for testing.
    async fn get_pool() -> Pool<Db> {
        Pool::connect(":memory:").await.unwrap()
    }

    /// Verifies that `Database::with_migration` can be called multiple times
    /// on the same pool without error.
    ///
    /// This confirms that migrations are idempotent — i.e., calling them again
    /// does not fail or break schema assumptions.
    #[tokio::test]
    async fn test_migration_idempotency() {
        let pool = get_pool().await;

        Database::with_migration(pool.clone()).await.unwrap();
        Database::with_migration(pool.clone()).await.unwrap();
    }

    /// Ensures that inserting the same image multiple times does not result in error.
    ///
    /// This tests both insertion success and idempotency:
    /// `ensure_image` should silently succeed even if the image already exists.
    #[tokio::test]
    async fn test_ensure_image() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        assert!(db.ensure_image(&image).await.is_ok());
        assert!(db.ensure_image(&image).await.is_ok());
    }

    #[tokio::test]
    async fn test_ensure_source() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        assert!(db.ensure_image_has_source(&image, "src").await.is_ok());
        assert_eq!(
            Some("src".to_string()),
            db.get_source(&image).await.unwrap()
        );
    }

    /// Ensures that inserting the same image multiple times does not result in error.
    ///
    /// This tests both insertion success and idempotency:
    /// `ensure_image` should silently succeed even if the image already exists.
    #[tokio::test]
    async fn test_ensure_metadata() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();
        let metadata = ImageMetadata {
            width: 200,
            height: 200,
            format: "image/png".to_string(),
            color_type: "rgba".to_string(),
            file_size: 1337,
            created_at: Some(DateTime::from_str("2025-05-02T01:18:49.678809123Z").unwrap()),
        };

        assert!(
            db.ensure_image_has_metadata(&image, &metadata)
                .await
                .is_ok()
        );
        assert!(
            db.ensure_image_has_metadata(&image, &metadata)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_ensure_metadata_without_created_at() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();
        let metadata = ImageMetadata {
            width: 200,
            height: 200,
            format: "image/png".to_string(),
            color_type: "rgba".to_string(),
            file_size: 1337,
            created_at: None,
        };

        assert!(
            db.ensure_image_has_metadata(&image, &metadata)
                .await
                .is_ok()
        );
        assert!(db.get_metadata(&image).await.unwrap().is_some());
    }

    /// Full test of tag operations:
    /// - Add tags to an image
    /// - Add duplicate tags safely
    /// - Remove tags idempotently
    /// - Verify final tag list is correct
    #[tokio::test]
    async fn test_operate_image_tag() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image = PixelHash::try_from("329435e5e66be809").unwrap();

        // Add tags "cat" and "dog" (including duplicate insertions)
        assert!(db.ensure_image_has_tag(&image, "cat").await.is_ok());
        assert!(db.ensure_image_has_tag(&image, "cat").await.is_ok());
        assert!(db.ensure_image_has_tag(&image, "dog").await.is_ok());

        // Confirm both tags are present
        assert_eq!(
            vec!["cat".to_string(), "dog".to_string()],
            db.get_tags(&image).await.unwrap()
        );

        // Remove "dog" tag twice (should be safe and idempotent)
        assert!(db.ensure_tag_removed(&image, "dog").await.is_ok());
        assert!(db.ensure_tag_removed(&image, "dog").await.is_ok());

        // Confirm only "cat" remains
        assert_eq!(vec!["cat".to_string()], db.get_tags(&image).await.unwrap());
    }

    #[tokio::test]
    async fn test_query_image() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        let image_cat = PixelHash::try_from("329435e5e66be809").unwrap();
        let image_dog = PixelHash::try_from("229435e5e66be809").unwrap();
        let image_cat_and_dog = PixelHash::try_from("129435e5e66be809").unwrap();

        assert!(db.ensure_image_has_tag(&image_cat, "cat").await.is_ok());
        assert!(db.ensure_image_has_tag(&image_dog, "dog").await.is_ok());
        assert!(
            db.ensure_image_has_tag(&image_cat_and_dog, "cat")
                .await
                .is_ok()
        );
        assert!(
            db.ensure_image_has_tag(&image_cat_and_dog, "dog")
                .await
                .is_ok()
        );

        let query_cat = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("cat")));
        let query_dog = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("dog")));
        let query_cat_and_dog = ImageQuery::new(ImageQueryKind::Where(
            ImageQueryExpr::tag("cat").and(ImageQueryExpr::tag("dog")),
        ));

        assert_eq!(
            vec![image_cat_and_dog.clone(), image_cat.clone()],
            db.query_image(query_cat).await.unwrap()
        );

        assert_eq!(
            vec![image_cat_and_dog.clone(), image_dog.clone(),],
            db.query_image(query_dog).await.unwrap()
        );

        assert_eq!(
            vec![image_cat_and_dog],
            db.query_image(query_cat_and_dog).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_query_tags() {
        let pool = get_pool().await;
        let db = Database::with_migration(pool.clone()).await.unwrap();

        assert!(db.ensure_tag("cat").await.is_ok());
        assert!(db.ensure_tag("dog").await.is_ok());

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
    }
}
