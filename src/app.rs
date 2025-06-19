//! # Image Archival System Module
//!
//! This module provides a high-level interface for managing image archival operations
//! including uploading, storing, tagging, and associating metadata and source URLs
//! within a robust asynchronous framework. It defines core structures and functions
//! necessary for handling the entire lifecycle of images within a scalable system.
//!
//! ## Provided Structures
//!
//! - **ArchiveImageCommand**: Central to creating and executing image archival requests,
//!   this struct facilitates the inclusion of tags and source information while managing
//!   the invocation of storage and database procedures.
//! - **Image**: Represents a comprehensive image model that bundles file path, hash,
//!   metadata, tags, and optional source information, capturing all details needed
//!   for managing and retrieving images.
//!
//! ## Core Asynchronous Functions
//!
//! - **execute**: Archives an image by storing it, extracting metadata, and updating
//!   the database with associated tags and source URLs if provided.
//! - **attach_tags**: Synchronizes and updates tag associations for a given image hash,
//!   efficiently calculating differences and applying updates in parallel.
//! - **attach_source**: Updates source information for an image in the database,
//!   ensuring accurate attribution of origin points for stored images.
//! - **remove_image**: Completely deletes an image from both storage and database,
//!   handling cleanup of records and metadata to maintain consistency.
//! - **find_image_by_hash**: Retrieves a full image model by its hash, consolidating
//!   metadata, tags, and file path, encapsulating all necessary image information.
//!
//! - **query_image** and **count_image**: Execute filtered queries on images, efficiently
//!   retrieving or counting matches based on conditions defined by `ImageQuery` objects.
//!
//! ## Error Handling
//!
//! Defines a robust error system with `AppError` enum, encapsulating storage, database,
//! and other custom errors to promote clear and manageable error management
//! throughout image operations.

use crate::{
    database::{Database, DatabaseError},
    query::{ImageQuery, TagQuery},
    storage::{ImageMetadata, MediaPath, PixelHash, Storage, StorageError},
};
use std::collections::{HashMap, HashSet};
use tokio::task::JoinSet;

/// Represents a command for archiving an image into the system.
///
/// This structure holds the raw image bytes, optional source URL, and associated tags.
/// Use builder-style methods (`with_tags`, `with_source`) to set additional information
/// before calling `execute()` to perform the archival process.
pub struct ArchiveImageCommand {
    /// Raw image bytes.
    pub bytes: Vec<u8>,
    /// Tags associated with the image.
    pub tags: Vec<String>,
    /// An optional source URL indicating the origin of the image.
    pub source: Option<String>,
}

impl ArchiveImageCommand {
    /// Creates a new `ArchiveImageCommand` with the provided raw image bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - A byte slice representing the raw image data.
    ///
    /// # Returns
    ///
    /// Returns a new `ArchiveImageCommand` instance.
    pub fn new(bytes: &[u8]) -> Self {
        ArchiveImageCommand {
            bytes: bytes.to_vec(),
            tags: vec![],
            source: None,
        }
    }

    /// Adds tags to the image command.
    ///
    /// # Arguments
    ///
    /// * `tags` - An iterator over strings, each representing a tag to attach.
    ///
    /// # Returns
    ///
    /// Returns the modified `ArchiveImageCommand` with updated tags.
    pub fn with_tags<T: IntoIterator<Item = String>>(mut self, tags: T) -> Self {
        self.tags = tags.into_iter().collect();
        self
    }

    /// Sets an optional source URL for the image.
    ///
    /// # Arguments
    ///
    /// * `src` - A string slice representing the source URL.
    ///
    /// # Returns
    ///
    /// Returns the modified `ArchiveImageCommand` with the source set.
    pub fn with_source(mut self, src: &str) -> Self {
        self.source = Some(src.to_string());
        self
    }

    /// Executes the archival process for the image.
    ///
    /// This involves storing the image, extracting metadata, inserting a database record,
    /// and attaching tags and an optional source URL if provided.
    ///
    /// # Arguments
    ///
    /// * `storage` - Reference to the storage system where the image will be stored.
    /// * `db` - Reference to the database where metadata and other information will be recorded.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the full `Image` model upon success or an `AppError` on failure.
    pub async fn execute(self, storage: &Storage, db: &Database) -> Result<Media, AppError> {
        let hash = match storage.create_file(&self.bytes) {
            Ok(hash) => Ok(hash),
            Err(e) => match &e {
                StorageError::HashCollision { hash, .. } => {
                    if !db.image_exists(hash).await? {
                        Ok(hash.clone())
                    } else {
                        Err(e)
                    }
                }
                _ => Err(e),
            },
        }?;
        let metadata = storage.get_metadata(&hash)?;

        let result = {
            db.ensure_image(&hash).await?;
            db.ensure_image_has_metadata(&hash, &metadata).await?;

            if !self.tags.is_empty() {
                attach_tags(
                    db,
                    storage,
                    &hash,
                    &self.tags.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                )
                .await?;
            }

            if let Some(src) = self.source {
                attach_source(db, storage, &hash, &src).await?;
            }

            find_image_by_hash(db, storage, &hash).await
        };

        match result {
            Ok(ok) => Ok(ok),
            Err(e) => {
                remove_image(storage, db, hash).await?;
                Err(e)
            }
        }
    }
}

/// Synchronizes the tag state of a given image hash with the provided desired tag list.
///
/// This function computes the difference between current tags in the database and desired tags,
/// adding or removing tags accordingly using parallel execution.
///
/// # Arguments
///
/// * `db` - Reference to the database where tag operations will be performed.
/// * `storage` - Reference to the storage for ensuring the image file presence.
/// * `hash` - The hash of the image to modify.
/// * `tags` - A slice of string slices representing the desired tags.
///
/// # Returns
///
/// Returns a `Result` indicating success or an `AppError` if an error occurred.
pub async fn attach_tags(
    db: &Database,
    storage: &Storage,
    hash: &PixelHash,
    tags: &[&str],
) -> Result<(), AppError> {
    if storage.index_file(hash).is_none() {
        return Err(AppError::StorageNotFound { hash: hash.clone() });
    }

    let desired: HashSet<&str> = tags.iter().copied().collect();
    let current = db.get_tags(hash).await?;
    let current: HashSet<&str> = current.iter().map(|f| f.as_str()).collect();

    let to_add: Vec<&str> = desired.difference(&current).copied().collect();
    let to_remove: Vec<&str> = current.difference(&desired).copied().collect();

    db.ensure_image_has_tags(hash, to_add.as_slice()).await?;
    db.ensure_tags_removed(hash, to_remove.as_slice()).await?;

    Ok(())
}

/// Updates the source information for a specific image in the database.
///
/// # Arguments
///
/// * `db` - Reference to the database where the source update will be applied.
/// * `storage` - Reference to the storage for ensuring the image file presence.
/// * `hash` - The hash of the image to be updated.
/// * `src` - The new source string to associate with the image.
///
/// # Returns
///
/// Returns a `Result` indicating success or an `AppError` if an error occurs.
pub async fn attach_source(
    db: &Database,
    storage: &Storage,
    hash: &PixelHash,
    src: &str,
) -> Result<(), AppError> {
    if storage.index_file(hash).is_none() {
        return Err(AppError::StorageNotFound { hash: hash.clone() });
    }

    db.ensure_image(hash).await?;
    db.ensure_image_has_source(hash, src).await?;

    Ok(())
}

/// Completely removes an image from both storage and the database.
///
/// # Arguments
///
/// * `storage` - Reference to the storage to handle file deletion.
/// * `db` - Reference to the database to handle record and metadata removal.
/// * `hash` - The hash of the image to remove.
///
/// # Returns
///
/// Returns a `Result` indicating success or an `AppError` if an error occurs.
pub async fn remove_image(
    storage: &Storage,
    db: &Database,
    hash: PixelHash,
) -> Result<(), AppError> {
    storage.ensure_deleted(&hash)?;
    db.ensure_image_removed(&hash).await?;

    Ok(())
}

/// Retrieves a full image model by its hash.
///
/// This function loads the file path from storage, retrieves metadata and tags
/// from the database, and includes any available source information.
///
/// # Arguments
///
/// * `db` - Reference to the database to retrieve image-related information.
/// * `storage` - Reference to the storage to locate the image file.
/// * `hash` - The hash of the image to retrieve.
///
/// # Returns
///
/// Returns a `Result` containing the complete `Image` structure or an `AppError` if retrieval fails.
pub async fn find_image_by_hash(
    db: &Database,
    storage: &Storage,
    hash: &PixelHash,
) -> Result<Media, AppError> {
    let path = storage
        .index_file(hash)
        .ok_or_else(|| AppError::StorageNotFound { hash: hash.clone() })?;

    let tags = db.get_tags(hash).await?;

    let metadata = db.get_metadata(hash).await?.unwrap_or_default();

    let source = db.get_source(hash).await?;

    Ok(Media {
        path,
        hash: hash.clone(),
        tags,
        metadata,
        source,
    })
}

/// Queries images using a filter and retrieves full `Image` structs for each match.
///
/// Metadata, tags, and source information are loaded in parallel to improve efficiency.
///
/// # Arguments
///
/// * `db` - Reference to the database where the query will be executed.
/// * `storage` - Reference to the storage system for image file access.
/// * `query` - An `ImageQuery` object representing the filtering criteria.
///
/// # Returns
///
/// Returns a `Result` containing a vector of `Image` structs or an `AppError` if the query fails.
pub async fn query_image(
    db: &Database,
    storage: &Storage,
    query: ImageQuery,
) -> Result<Vec<Media>, AppError> {
    let hashes = db.query_image(query).await?;

    let mut set = JoinSet::new();
    for hash in hashes.clone() {
        let db = db.clone();
        let storage = storage.clone();
        set.spawn(async move {
            let image = find_image_by_hash(&db, &storage, &hash).await?;
            Ok::<(PixelHash, Media), AppError>((hash, image))
        });
    }

    let mut map = HashMap::new();
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok((hash, image))) => {
                map.insert(hash, image);
            }
            Ok(Err(e)) => return Err(e),
            Err(join_err) => panic!("task panicked in image retrieval: {join_err}"),
        }
    }

    let images = hashes.into_iter().filter_map(|h| map.remove(&h)).collect();

    Ok(images)
}

/// Counts the number of images matching a given query.
///
/// # Arguments
///
/// * `db` - Reference to the database where the counting operation will occur.
/// * `query` - An `ImageQuery` object representing the filtering criteria.
///
/// # Returns
///
/// Returns a `Result` containing the count of matching images or an `AppError` if counting fails.
pub async fn count_image(db: &Database, query: ImageQuery) -> Result<u64, AppError> {
    Ok(db.count_image(query).await?)
}

/// Counts the number of images that are associated with a specific tag.
///
/// This function executes a counting operation in the database to determine the quantity
/// of images that have been tagged with a given tag name. It provides a convenient way
/// to assess the prevalence of a specific tag within the image dataset.
///
/// # Arguments
///
/// * `db` - Reference to the database within which the counting operation will be performed.
/// * `tag` - A string slice representing the tag name for which the image count is desired.
///
/// # Returns
///
/// Returns a `Result` containing either the count of images with the specified tag as an `u64`
/// or an `AppError` if an error occurs during the counting process.
pub async fn count_image_by_tag(db: &Database, tag: &str) -> Result<u64, AppError> {
    Ok(db.count_image_by_tag(tag).await?)
}

/// Refreshes the image count in the database.
///
/// This function triggers a recalculation of the total number
/// of images stored in the database. It ensures that the image
/// count reflects the current state of image records, including
/// any recent additions or deletions. Executing this function
/// helps maintain accurate statistics about the image collection.
///
/// # Arguments
///
/// * `db` - Reference to the database where the image count should be refreshed.
///
/// # Returns
///
/// Returns a `Result` indicating success or an `AppError` if an error occurs during the count refresh process.
pub async fn refresh_count(db: &Database) -> Result<(), AppError> {
    Ok(db.refresh_image_count().await?)
}

/// Executes a tag query against the database and returns matching tag names.
///
/// # Arguments
///
/// * `db` - Reference to the database to execute the tag query.
/// * `query` - A `TagQuery` object representing the filtering criteria for tags.
///
/// # Returns
///
/// Returns a `Result` containing a vector of tag names as strings or an `AppError`
pub async fn query_tags(db: &Database, query: TagQuery) -> Result<Vec<String>, AppError> {
    db.query_tags(query).await.map_err(AppError::from)
}

/// Represents a complete image with associated metadata, tags, and optional source information.
///
/// This structure holds the file path, hash, metadata, and other attributes required to fully
/// describe an image within the system.
#[derive(Debug, Clone, PartialEq)]
pub struct Media {
    /// The file path where the image is stored.
    pub path: MediaPath,
    /// The unique hash representing the image.
    pub hash: PixelHash,
    /// Metadata associated with the image.
    pub metadata: ImageMetadata,
    /// Tags associated with the image.
    pub tags: Vec<String>,
    /// An optional source URL indicating where the image came from.
    pub source: Option<String>,
}

/// Error types within the application, encapsulating storage, database, and other custom errors.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("database error: {0}")]
    Database(#[from] DatabaseError),

    #[error("image not found: {hash}")]
    StorageNotFound { hash: PixelHash },
}

#[cfg(test)]
mod tests {
    use crate::{
        app::{ArchiveImageCommand, attach_tags, find_image_by_hash, query_image, remove_image},
        database::{Database, MIGRATOR, Pool},
        query::{ImageQuery, ImageQueryExpr, ImageQueryKind},
        storage::Storage,
    };
    use tempfile::TempDir;

    fn get_storage() -> Storage {
        let tmp_dir = TempDir::new().unwrap();
        Storage::new(tmp_dir.path().to_path_buf())
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_query(pool: Pool) {
        let db = Database::new(pool);
        let storage = get_storage();
        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");

        ArchiveImageCommand::new(file_bytes)
            .with_tags(["cat".to_string()])
            .with_source("https://example.com")
            .execute(&storage, &db)
            .await
            .unwrap();

        let query = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("cat")));

        let res = query_image(&db, &storage, query).await.unwrap();

        dbg!(res);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_remove_image(pool: Pool) {
        let db = Database::new(pool);
        let storage = get_storage();
        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");

        let image = ArchiveImageCommand::new(file_bytes)
            .with_tags(["cat".to_string()])
            .with_source("https://example.com")
            .execute(&storage, &db)
            .await
            .unwrap();

        remove_image(&storage, &db, image.hash).await.unwrap();
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_attach_tags(pool: Pool) {
        let db = Database::new(pool);
        let storage = get_storage();
        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");

        let image = ArchiveImageCommand::new(file_bytes)
            .with_tags(["cat".to_string(), "scary".to_string()])
            .with_source("https://example.com")
            .execute(&storage, &db)
            .await
            .unwrap();

        let desired = &["cat", "cute"];

        attach_tags(&db, &storage, &image.hash, desired)
            .await
            .unwrap();

        assert_eq!(
            desired.to_vec(),
            find_image_by_hash(&db, &storage, &image.hash)
                .await
                .unwrap()
                .tags
        );
    }
}
