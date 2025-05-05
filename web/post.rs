use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use buru::{
    app::{AppError, Image, find_image_by_hash, query_image},
    query::{self, QueryKind},
    storage::Md5Hash,
};
use serde::{Deserialize, Serialize};

use crate::{AppConfig, AppState};

#[derive(Deserialize)]
pub struct PostQuery {
    tags: Option<String>, // e.g. "cute cat"
    page: Option<u32>,
    limit: Option<u32>,
}

#[derive(Serialize, Debug)]
pub struct PostResponse {
    pub id: u128,
    pub created_at: String,
    pub updated_at: String,
    pub uploader_id: u32,
    pub approver_id: Option<u32>,
    pub tag_string: String,
    pub tag_string_general: String,
    pub tag_string_artist: String,
    pub tag_string_copyright: String,
    pub tag_string_character: String,
    pub tag_string_meta: String,
    pub rating: String,
    pub parent_id: Option<u32>,
    pub pixiv_id: Option<u32>,
    pub source: String,
    pub md5: Option<String>,
    pub file_url: Option<String>,
    pub large_file_url: Option<String>,
    pub preview_file_url: Option<String>,
    pub file_ext: String,
    pub file_size: u32,
    pub image_width: u32,
    pub image_height: u32,
    pub score: i32,
    pub up_score: i32,
    pub down_score: i32,
    pub fav_count: u32,
    pub tag_count_general: u32,
    pub tag_count_artist: u32,
    pub tag_count_copyright: u32,
    pub tag_count_character: u32,
    pub tag_count_meta: u32,
    pub last_comment_bumped_at: Option<String>,
    pub last_noted_at: Option<String>,
    pub has_large: bool,
    pub has_children: bool,
    pub has_visible_children: bool,
    pub has_active_children: bool,
    pub is_banned: bool,
    pub is_deleted: bool,
    pub is_flagged: bool,
    pub is_pending: bool,
    pub bit_flags: u32,
}

impl PostResponse {
    fn from_image(config: AppConfig, value: Image) -> Self {
        let file_url = config
            .cdn_base_url
            .join(value.path)
            .to_string_lossy()
            .to_string();
        PostResponse {
            id: value.hash.clone().into(),
            tag_string: value.tags.join(" "),
            file_url: Some(file_url.to_string()),
            created_at: value.metadata.created_at.to_rfc3339(),
            updated_at: value.metadata.created_at.to_rfc3339(),
            uploader_id: 0,
            approver_id: None,
            tag_string_general: value.tags.join(" "),
            tag_string_artist: "".to_string(),
            tag_string_copyright: "".to_string(),
            tag_string_character: "".to_string(),
            tag_string_meta: "".to_string(),
            rating: "e".to_string(),
            parent_id: None,
            pixiv_id: None,
            source: value.source.unwrap_or_default(),
            md5: Some(value.hash.to_string()),
            large_file_url: Some(file_url.to_string()),
            preview_file_url: None,
            file_ext: value.metadata.format,
            file_size: value.metadata.file_size as u32,
            image_width: value.metadata.width,
            image_height: value.metadata.height,
            score: 0,
            up_score: 0,
            down_score: 0,
            fav_count: 0,
            tag_count_general: value.tags.len() as u32,
            tag_count_artist: 0,
            tag_count_copyright: 0,
            tag_count_character: 0,
            tag_count_meta: 0,
            last_comment_bumped_at: None,
            last_noted_at: None,
            has_large: true,
            has_children: false,
            has_visible_children: false,
            has_active_children: false,
            is_banned: false,
            is_deleted: false,
            is_flagged: false,
            is_pending: false,
            bit_flags: 0,
        }
    }
}

pub async fn get_posts(
    State(app): State<AppState>,
    Query(params): Query<PostQuery>,
) -> Result<Json<Vec<PostResponse>>, PostError> {
    let tags = params
        .tags
        .unwrap_or_default()
        .split_whitespace()
        .map(String::from)
        .collect::<Vec<_>>();

    let query = query::Query::new(
        tags.into_iter()
            .map(query::QueryExpr::Tag)
            .reduce(query::QueryExpr::and)
            .map(|expr| QueryKind::Where(expr))
            .unwrap_or(QueryKind::All),
    )
    .with_limit(params.limit.unwrap_or(20))
    .with_offset((params.page.unwrap_or(1) - 1) * params.limit.unwrap_or(20));

    let results = query_image(&app.db, &app.storage, query).await?;

    Ok(Json(
        results
            .into_iter()
            .map(|image| PostResponse::from_image(app.config.clone(), image))
            .collect(),
    ))
}

pub async fn get_post(
    State(app): State<AppState>,
    Path(id): Path<u128>,
) -> Result<Json<PostResponse>, PostError> {
    let hash = Md5Hash::from(id);

    let image = find_image_by_hash(&app.db, &app.storage, hash)
        .await
        .map_err(|_e| PostError)?;

    Ok(Json(PostResponse::from_image(app.config, image)))
}

pub struct PostError;

impl From<AppError> for PostError {
    fn from(value: AppError) -> Self {
        todo!()
    }
}

impl IntoResponse for PostError {
    fn into_response(self) -> axum::response::Response {
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                message: "".to_string(),
            }),
        )
            .into_response()
    }
}
