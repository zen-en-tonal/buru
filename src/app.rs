use crate::{
    database::{Database, DatabaseError},
    query::Query,
    storage::{ImageMetadata, PixelHash, Storage, StorageError},
};
use std::{collections::HashMap, path::PathBuf};
use tokio::task::JoinSet;

pub struct ArchiveImageCommand {
    pub bytes: Vec<u8>,
    pub tags: Vec<String>,
    pub source: Option<String>,
}

impl ArchiveImageCommand {
    pub fn new(bytes: &[u8]) -> Self {
        ArchiveImageCommand {
            bytes: bytes.to_vec(),
            tags: vec![],
            source: None,
        }
    }

    pub fn with_tags<T: IntoIterator<Item = String>>(mut self, tags: T) -> Self {
        self.tags = tags.into_iter().collect();

        self
    }

    pub fn with_source(mut self, src: &str) -> Self {
        self.source = Some(src.to_string());

        self
    }

    pub async fn execute(self, storage: &Storage, db: &Database) -> Result<Image, AppError> {
        let hash = storage.create_file(&self.bytes)?;
        let metadata = storage.get_metadata(&hash)?;

        db.ensure_image(&hash).await?;
        db.ensure_image_has_metadata(&hash, &metadata).await?;

        if !self.tags.is_empty() {
            attach_tags(db, &hash, &self.tags).await?;
        }

        if let Some(src) = self.source {
            attach_source(db, &hash, &src).await?;
        }

        find_image_by_hash(db, storage, hash).await
    }
}

pub async fn attach_tags(db: &Database, hash: &PixelHash, tags: &[String]) -> Result<(), AppError> {
    let mut set = JoinSet::new();

    for tag in tags {
        let db = db.clone();
        let hash = hash.clone();
        let tag = tag.to_string();
        set.spawn(async move { db.ensure_image_has_tag(&hash, &tag).await });
    }

    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(())) => (),
            Ok(Err(e)) => return Err(AppError::Database(e)),
            Err(join_err) => panic!("task panicked in image retrieval: {join_err}"),
        }
    }

    Ok(())
}

pub async fn attach_source(db: &Database, hash: &PixelHash, src: &str) -> Result<(), AppError> {
    db.ensure_image(hash).await?;
    db.ensure_image_has_source(hash, src).await?;

    Ok(())
}

pub async fn remove_image(
    storage: &Storage,
    db: &Database,
    hash: PixelHash,
) -> Result<(), AppError> {
    db.ensure_image_removed(&hash).await?;
    storage.ensure_deleted(&hash)?;

    Ok(())
}

pub async fn find_image_by_hash(
    db: &Database,
    storage: &Storage,
    hash: PixelHash,
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
        hash,
        tags,
        metadata,
        source,
    })
}

pub async fn query_image(
    db: &Database,
    storage: &Storage,
    query: Query,
) -> Result<Vec<Image>, AppError> {
    // Step 1: Execute the query to get matching image hashes, in order
    let hashes = db.find_by_query(query).await?;

    // Step 2: Spawn parallel tasks to retrieve images by hash
    let mut set = JoinSet::new();
    for hash in hashes.clone() {
        let db = db.clone();
        let storage = storage.clone();
        set.spawn(async move {
            // Load image data (from storage and metadata from DB) by hash
            let image = find_image_by_hash(&db, &storage, hash.clone()).await?;
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
        app::{ArchiveImageCommand, query_image, remove_image},
        database::{Database, Pool},
        query::{Query, QueryExpr, QueryKind},
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

        let query = Query::new(QueryKind::Where(QueryExpr::tag("cat")));

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
}
