use crate::{
    database::{Database, DatabaseError},
    query::Query,
    storage::{Md5Hash, Storage, StorageError},
};
use std::{collections::HashMap, path::PathBuf};
use tokio::task::JoinSet;

pub async fn archive_image(
    storage: &Storage,
    db: &Database,
    bytes: &[u8],
    tags: &[String],
) -> Result<Md5Hash, AppError> {
    let hash = storage.create_file(bytes)?;

    db.ensure_image(&hash).await?;
    for tag in tags {
        db.ensure_image_has_tag(&hash, tag).await?;
    }

    Ok(hash)
}

pub async fn remove_image(storage: &Storage, db: &Database, hash: Md5Hash) -> Result<(), AppError> {
    db.ensure_image_removed(&hash).await?;
    storage.ensure_deleted(&hash)?;

    Ok(())
}

pub async fn find_image_by_hash(
    db: &Database,
    storage: &Storage,
    hash: Md5Hash,
) -> Result<Image, AppError> {
    let path = storage
        .index_file(&hash)
        .ok_or_else(|| AppError::StorageNotFound { hash: hash.clone() })?;

    let tags = db.get_tags(&hash).await?;

    Ok(Image { path, hash, tags })
}

pub async fn query_image(
    db: &Database,
    storage: &Storage,
    query: Query,
) -> Result<Vec<Image>, AppError> {
    let hashes = db.find_by_query(query).await?;

    let mut set = JoinSet::new();
    for hash in hashes.clone() {
        let db = db.clone();
        let storage = storage.clone();
        set.spawn(async move {
            let image = find_image_by_hash(&db, &storage, hash.clone()).await?;

            Ok::<(Md5Hash, Image), AppError>((hash, image))
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

    let images = hashes
        .into_iter()
        .filter_map(|h| map.remove(&h)) // to avoid duplication.
        .collect();

    Ok(images)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Image {
    path: PathBuf,
    hash: Md5Hash,
    tags: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("database error: {0}")]
    Database(#[from] DatabaseError),

    #[error("image not found: {hash}")]
    StorageNotFound { hash: Md5Hash },
}

#[cfg(test)]
mod tests {
    use crate::{
        app::{Image, archive_image, query_image},
        database::{Database, Pool},
        query::{Query, QueryExpr},
        storage::Storage,
    };
    use std::path::PathBuf;
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
        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");

        let hash = archive_image(&storage, &db, file_bytes, &["cat".to_string()])
            .await
            .unwrap();

        let query = Query::new(QueryExpr::tag("cat"));

        assert_eq!(
            vec![Image {
                path: PathBuf::from("62/0a/620a139c9d3e63188299d0150c198bd5.png"),
                hash,
                tags: vec!["cat".to_string()]
            }],
            query_image(&db, &storage, query).await.unwrap()
        );
    }
}
