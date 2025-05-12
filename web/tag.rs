use std::collections::HashMap;

use crate::AppState;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use buru::prelude::*;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use twox_hash::XxHash64;

#[allow(overflowing_literals)]
fn compute_hash(tag: &str) -> i64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(tag.as_bytes());

    (hasher.finish() as i64) ^ 0x8000_0000_0000_0000
}

#[derive(Deserialize)]
pub struct TagQuery {
    #[serde(rename = "search[name_comma]")]
    tags: Option<String>,
    page: Option<u32>,
    limit: Option<u32>,
}

#[derive(Serialize, Debug)]
pub struct TagResponse {
    pub id: i64,
    pub name: String,
    pub post_count: u64,
    pub created_at: String,
    pub updated_at: String,
    pub is_deprecated: bool,
    pub words: Vec<String>,
}

impl TagResponse {
    fn from(value: &str, count: u64) -> Self {
        Self {
            id: compute_hash(value),
            name: value.to_string(),
            post_count: count,
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
            is_deprecated: false,
            words: value.split("_").into_iter().map(String::from).collect(),
        }
    }
}

pub async fn get_tags(
    State(app): State<AppState>,
    Query(params): Query<TagQuery>,
) -> Result<Json<Vec<TagResponse>>, TagError> {
    let tags = params
        .tags
        .unwrap_or_default()
        .split(",")
        .filter(|e| *e != "")
        .map(String::from)
        .collect::<Vec<_>>();

    let query = buru::query::TagQuery::new(
        tags.into_iter()
            .map(TagQueryExpr::Exact)
            .reduce(TagQueryExpr::or)
            .map(TagQueryKind::Where)
            .unwrap_or(TagQueryKind::All),
    )
    .with_limit(params.limit.unwrap_or(20))
    .with_offset((params.page.unwrap_or(1) - 1) * params.limit.unwrap_or(20));

    let tags = query_tags(&app.db, query).await?;
    let tags: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
    let counts = tag_counts(&app.db, tags.as_slice()).await?;
    let resp: Vec<TagResponse> = tags
        .into_iter()
        .map(|tag| TagResponse::from(tag, *counts.get(tag).unwrap_or(&0)))
        .collect();

    Ok(Json(resp))
}

#[derive(Deserialize)]
pub struct SuggestTagQuery {
    #[serde(rename = "search[query]")]
    looking_for: Option<String>,
    limit: Option<u32>,
}

#[derive(Serialize, Debug)]
pub struct SuggestTagResponse {
    #[serde(rename = "type")]
    pub tag_type: String,
    pub label: String,
    pub value: String,
    pub category: u8,
    pub post_count: u64,
}

async fn tag_counts(db: &Database, tags: &[&str]) -> Result<HashMap<String, u64>, TagError> {
    let mut set = tokio::task::JoinSet::new();

    for tag in tags.iter() {
        let db = db.clone();
        let tag = tag.to_string();
        set.spawn(async move {
            let count = count_image_by_tag(&db, &tag).await?;
            Ok::<(String, u64), TagError>((tag, count))
        });
    }

    let mut map = HashMap::new();
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok((tag, count))) => {
                map.insert(tag, count);
            }
            Ok(Err(e)) => return Err(e),
            Err(join_err) => panic!("task panicked in post count retrieval: {join_err}"),
        }
    }

    Ok(map)
}

impl SuggestTagResponse {
    fn from(tag: &str, count: u64) -> Self {
        Self {
            tag_type: "tag-word".to_string(),
            label: tag.replace("_", " "),
            value: tag.to_string(),
            category: 0,
            post_count: count,
        }
    }
}

pub async fn suggest_tags(
    State(app): State<AppState>,
    Query(params): Query<SuggestTagQuery>,
) -> Result<Json<Vec<SuggestTagResponse>>, TagError> {
    let query = buru::query::TagQuery::new(
        params
            .looking_for
            .map(TagQueryExpr::Prefix)
            .map(TagQueryKind::Where)
            .unwrap_or(TagQueryKind::All),
    )
    .with_limit(params.limit.unwrap_or(20));

    let tags = query_tags(&app.db, query).await?;
    let tags: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
    let counts = tag_counts(&app.db, tags.as_slice()).await?;
    let resp: Vec<SuggestTagResponse> = tags
        .into_iter()
        .map(|tag| SuggestTagResponse::from(tag, *counts.get(tag).unwrap_or(&0)))
        .collect();

    Ok(Json(resp))
}

pub async fn refresh_count(State(app): State<AppState>) -> Result<StatusCode, TagError> {
    buru::app::refresh_count(&app.db).await?;

    Ok(StatusCode::OK)
}

pub enum TagError {
    App(AppError),
    // BadRequest(String),
}

impl From<AppError> for TagError {
    fn from(value: AppError) -> Self {
        TagError::App(value)
    }
}

impl IntoResponse for TagError {
    fn into_response(self) -> axum::response::Response {
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        let (status, message) = match self {
            TagError::App(app_error) => match app_error {
                AppError::Storage(storage_error) => match storage_error {
                    StorageError::HashCollision { existing_path } => (
                        StatusCode::BAD_REQUEST,
                        existing_path.to_string_lossy().to_string(),
                    ),
                    StorageError::UnsupportedFile { kind } => (
                        StatusCode::BAD_REQUEST,
                        kind.map(|k| k.mime_type().to_string())
                            .unwrap_or("unknown".to_string()),
                    ),
                    StorageError::FileNotFound { hash } => {
                        (StatusCode::NOT_FOUND, hash.to_string())
                    }
                    StorageError::Io(error) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    }
                    StorageError::Image(image_error) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, image_error.to_string())
                    }
                },
                AppError::Database(database_error) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    database_error.to_string(),
                ),
                AppError::StorageNotFound { hash } => (StatusCode::NOT_FOUND, hash.to_string()),
            },
        };

        (status, Json(ErrorResponse { message })).into_response()
    }
}
