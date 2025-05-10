use crate::{
    database::{Database, DatabaseError},
    query::{ImageQuery, TagQuery},
    storage::{ImageMetadata, PixelHash, Storage, StorageError},
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use tokio::task::JoinSet;

/// Represents a command for archiving an image into the system.
///
/// This includes raw image bytes, optional source URL, and tags.
/// Use builder-style methods (`with_tags`, `with_source`) to enrich the command
/// before calling `execute`.
pub struct ArchiveImageCommand {
    /// Raw image bytes
    pub bytes: Vec<u8>,
    /// Tags to attach to the image
    pub tags: Vec<String>,
    /// Optional source URL (where the image came from)
    pub source: Option<String>,
}

impl ArchiveImageCommand {
    /// Creates a new ArchiveImageCommand with raw bytes.
    pub fn new(bytes: &[u8]) -> Self {
        ArchiveImageCommand {
            bytes: bytes.to_vec(),
            tags: vec![],
            source: None,
        }
    }

    /// Adds tags to the command.
    pub fn with_tags<T: IntoIterator<Item = String>>(mut self, tags: T) -> Self {
        self.tags = tags.into_iter().collect();

        self
    }

    /// Adds an optional source string to the command.
    pub fn with_source(mut self, src: &str) -> Self {
        self.source = Some(src.to_string());

        self
    }

    /// Executes the image archival process:
    /// - Stores the image
    /// - Extracts metadata
    /// - Inserts DB record
    /// - Adds tags and source if present
    /// - Returns the full `Image` model
    pub async fn execute(self, storage: &Storage, db: &Database) -> Result<Image, AppError> {
        let hash = storage.create_file(&self.bytes)?;
        let metadata = storage.get_metadata(&hash)?;

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
    }
}

/// Synchronizes tag state of a given image hash with the provided desired tag list.
///
/// Computes the diff between current DB tags and desired tags,
/// then adds/removes tags accordingly using parallel execution.
pub async fn attach_tags(
    db: &Database,
    storage: &Storage,
    hash: &PixelHash,
    tags: &[&str],
) -> Result<(), AppError> {
    if storage.index_file(hash).is_none() {
        return Err(AppError::StorageNotFound { hash: hash.clone() });
    }

    let desired: HashSet<&str> = tags.into_iter().copied().collect();
    let current = db.get_tags(hash).await?;
    let current: HashSet<&str> = current.iter().map(|f| f.as_str()).collect();

    let to_add: Vec<&str> = desired.difference(&current).into_iter().copied().collect();
    let to_remove: Vec<&str> = current.difference(&desired).into_iter().copied().collect();

    db.ensure_image_has_tags(&hash, to_add.as_slice()).await?;
    db.ensure_tags_removed(&hash, to_remove.as_slice()).await?;

    Ok(())
}

/// Updates the image source field in the database.
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

/// Completely removes an image from both storage and database.
pub async fn remove_image(
    storage: &Storage,
    db: &Database,
    hash: PixelHash,
) -> Result<(), AppError> {
    db.ensure_image_removed(&hash).await?;
    storage.ensure_deleted(&hash)?;

    Ok(())
}

/// Retrieves a full image model by hash.
///
/// This loads:
/// - File path from storage
/// - Metadata and tags from database
/// - Optional source
pub async fn find_image_by_hash(
    db: &Database,
    storage: &Storage,
    hash: &PixelHash,
) -> Result<Image, AppError> {
    let path = storage
        .index_file(&hash)
        .ok_or_else(|| AppError::StorageNotFound { hash: hash.clone() })?;

    let tags = db.get_tags(&hash).await?;

    let metadata = db
        .get_metadata(&hash)
        .await?
        .expect("Failed to get metadata");

    let source = db.get_source(&hash).await?;

    Ok(Image {
        path,
        hash: hash.clone(),
        tags,
        metadata,
        source,
    })
}

/// Queries images using a filter and retrieves full `Image` structs for each matching entry.
///
/// Loads metadata, tags, and source in parallel.
pub async fn query_image(
    db: &Database,
    storage: &Storage,
    query: ImageQuery,
) -> Result<Vec<Image>, AppError> {
    // Step 1: Execute the query to get matching image hashes, in order
    let hashes = db.query_image(query).await?;

    // Step 2: Spawn parallel tasks to retrieve images by hash
    let mut set = JoinSet::new();
    for hash in hashes.clone() {
        let db = db.clone();
        let storage = storage.clone();
        set.spawn(async move {
            // Load image data (from storage and metadata from DB) by hash
            let image = find_image_by_hash(&db, &storage, &hash).await?;
            Ok::<(PixelHash, Image), AppError>((hash, image))
        });
    }

    // Step 3: Collect results from all parallel tasks into a hash map
    let mut map = HashMap::new();
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok((hash, image))) => {
                map.insert(hash, image); // Store successfully loaded images
            }
            Ok(Err(e)) => return Err(e), // Propagate application-level error
            Err(join_err) => panic!("task panicked in image retrieval: {join_err}"),
        }
    }

    // Step 4: Preserve query order by using original hash list and extracting images in order
    let images = hashes
        .into_iter()
        .filter_map(|h| map.remove(&h)) // Skip if any image failed to load
        .collect();

    Ok(images)
}

/// Executes a tag query against the database and returns matching tag names.
pub async fn query_tags(db: &Database, query: TagQuery) -> Result<Vec<String>, AppError> {
    db.query_tags(query).await.map_err(AppError::from)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Image {
    pub path: PathBuf,
    pub hash: PixelHash,
    pub metadata: ImageMetadata,
    pub tags: Vec<String>,
    pub source: Option<String>,
}

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
        database::{Database, Pool},
        query::{ImageQuery, ImageQueryExpr, ImageQueryKind},
        storage::Storage,
    };
    use tempfile::TempDir;

    async fn get_db() -> Database {
        let pool = Pool::connect(":memory:").await.unwrap();
        Database::with_migration(pool.clone()).await.unwrap()
    }

    fn get_storage() -> Storage {
        let tmp_dir = TempDir::new().unwrap();
        Storage::new(tmp_dir.path().to_path_buf())
    }

    #[tokio::test]
    async fn test_query() {
        let db = get_db().await;
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

    #[tokio::test]
    async fn test_remove_image() {
        let db = get_db().await;
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

    #[tokio::test]
    async fn test_attach_tags() {
        let db = get_db().await;
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
        )
    }
}
