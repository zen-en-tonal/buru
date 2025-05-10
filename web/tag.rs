use crate::AppState;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use buru::{
    app::{AppError, query_tags},
    query::{self, TagQueryExpr, TagQueryKind},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct TagQuery {
    #[serde(rename = "search[name_comma]")]
    tags: Option<String>,
    page: Option<u32>,
    limit: Option<u32>,
}

#[derive(Serialize, Debug)]
pub struct TagResponse {
    pub id: u64,
    pub name: String,
    pub post_count: u64,
    pub created_at: String,
    pub updated_at: String,
    pub is_deprecated: bool,
    pub words: Vec<String>,
}

impl From<String> for TagResponse {
    fn from(value: String) -> Self {
        Self {
            id: 0,
            name: value.clone(),
            post_count: 0,
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

    let query = query::TagQuery::new(
        tags.into_iter()
            .map(TagQueryExpr::Exact)
            .reduce(TagQueryExpr::or)
            .map(TagQueryKind::Where)
            .unwrap_or(TagQueryKind::All),
    )
    .with_limit(params.limit.unwrap_or(20))
    .with_offset((params.page.unwrap_or(1) - 1) * params.limit.unwrap_or(20));

    let results = query_tags(&app.db, query).await?;

    Ok(Json(results.into_iter().map(TagResponse::from).collect()))
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

impl From<String> for SuggestTagResponse {
    fn from(value: String) -> Self {
        Self {
            tag_type: "tag-word".to_string(),
            label: value.replace("_", " "),
            value,
            category: 4,
            post_count: 0,
        }
    }
}

pub async fn suggest_tags(
    State(app): State<AppState>,
    Query(params): Query<SuggestTagQuery>,
) -> Result<Json<Vec<SuggestTagResponse>>, TagError> {
    let query = query::TagQuery::new(
        params
            .looking_for
            .map(TagQueryExpr::Prefix)
            .map(TagQueryKind::Where)
            .unwrap_or(TagQueryKind::All),
    )
    .with_limit(params.limit.unwrap_or(20));

    let results = query_tags(&app.db, query).await?;

    Ok(Json(
        results.into_iter().map(SuggestTagResponse::from).collect(),
    ))
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
                    buru::storage::StorageError::HashCollision { existing_path } => (
                        StatusCode::CONFLICT,
                        existing_path.to_string_lossy().to_string(),
                    ),
                    buru::storage::StorageError::UnsupportedFile { kind } => (
                        StatusCode::BAD_REQUEST,
                        kind.map(|k| k.mime_type().to_string())
                            .unwrap_or("unknown".to_string()),
                    ),
                    buru::storage::StorageError::FileNotFound { hash } => {
                        (StatusCode::NOT_FOUND, hash.to_string())
                    }
                    buru::storage::StorageError::Io(error) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    }
                    buru::storage::StorageError::Image(image_error) => {
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
