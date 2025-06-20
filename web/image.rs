use crate::{AppConfig, AppState};
use axum::{
    Json,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use buru::{prelude::*, query};
use bytes::BytesMut;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct ImageQueryParam {
    tags: Option<String>, // e.g. "cute cat"
    page: Option<u32>,
    limit: Option<u32>,
}

#[derive(Serialize, Debug)]
pub struct ImageResponse {
    pub id: i64,
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
    pub media_asset: MediaAsset,
}

#[derive(Debug, Serialize)]
pub struct MediaAsset {
    pub id: i64,
    pub created_at: String,
    pub updated_at: String,
    pub md5: String,
    pub file_ext: String,
    pub file_size: u64,
    pub image_width: u32,
    pub image_height: u32,
    pub duration: Option<f64>,
    pub status: String,
    pub file_key: String,
    pub is_public: bool,
    pub pixel_hash: String,
    pub variants: Vec<Variant>,
}

impl MediaAsset {
    fn from_image(image: &Media, variants: &Variants) -> Self {
        let created_at = image
            .metadata
            .created_at
            .map(|e| e.to_rfc3339())
            .unwrap_or_default();
        let hash = image.clone().hash;

        Self {
            id: image.hash.clone().to_signed(),
            created_at: created_at.clone(),
            updated_at: created_at,
            md5: hash.clone().to_string(),
            file_ext: image.metadata.format.clone(),
            file_size: image.metadata.file_size,
            image_width: image.metadata.width,
            image_height: image.metadata.height,
            duration: image.metadata.duration,
            status: "active".to_string(),
            file_key: "bbD6k0WiU".to_string(),
            is_public: true,
            pixel_hash: hash.clone().to_string(),
            variants: variants.clone().into(),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Variant {
    #[serde(rename = "type")]
    pub variant_type: String,
    pub url: String,
    pub width: u32,
    pub height: u32,
    pub file_ext: String,
}

fn generate_variants(config: &AppConfig, org: &Media) -> Variants {
    let (original_path, preview_path) = match org.path {
        MediaPath::Image(ref path_buf) => (path_buf, path_buf),
        MediaPath::Video {
            ref video,
            ref thumb,
        } => (video, thumb),
    };

    Variants {
        preview: Variant {
            variant_type: "180x180".to_string(),
            url: config
                .cdn_base_url
                .join(PathBuf::from("180x180"))
                .join(preview_path)
                .to_string_lossy()
                .to_string(),
            width: 180,
            height: 180,
            file_ext: preview_path
                .extension()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        },
        large: Variant {
            variant_type: "sample".to_string(),
            url: config
                .cdn_base_url
                .join(format!(
                    "{}x{}",
                    org.metadata.width / 2,
                    org.metadata.height / 2
                ))
                .join(preview_path)
                .to_string_lossy()
                .to_string(),
            width: org.metadata.width / 2,
            height: org.metadata.height / 2,
            file_ext: preview_path
                .extension()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        },
        orig: Variant {
            variant_type: "original".to_string(),
            url: config
                .cdn_base_url
                .join("original")
                .join(original_path)
                .to_string_lossy()
                .to_string(),
            width: org.metadata.width,
            height: org.metadata.height,
            file_ext: original_path
                .extension()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        },
    }
}

#[derive(Debug, Clone)]
struct Variants {
    orig: Variant,
    large: Variant,
    preview: Variant,
}

impl From<Variants> for Vec<Variant> {
    fn from(val: Variants) -> Self {
        vec![val.preview, val.large, val.orig]
    }
}

impl ImageResponse {
    fn from_image(config: AppConfig, value: Media) -> Self {
        let created_at = value
            .metadata
            .created_at
            .map(|e| e.to_rfc3339())
            .unwrap_or_default();
        let variants = generate_variants(&config, &value);
        let asset = MediaAsset::from_image(&value, &variants);

        ImageResponse {
            id: value.hash.clone().to_signed(),
            tag_string: value.tags.join(" "),
            file_url: Some(variants.orig.url),
            created_at: created_at.clone(),
            updated_at: created_at.clone(),
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
            large_file_url: Some(variants.large.url),
            preview_file_url: Some(variants.preview.url),
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
            media_asset: asset,
        }
    }
}

impl From<ImageQueryParam> for query::ImageQuery {
    fn from(value: ImageQueryParam) -> Self {
        let tags = value
            .tags
            .unwrap_or_default()
            .split_whitespace()
            .map(String::from)
            .collect::<Vec<_>>();

        let mut exprs: Vec<query::ImageQueryExpr> = vec![];
        let mut order_by: Option<query::OrderBy> = None;

        for tag in tags {
            match tag.as_str() {
                negate if tag.starts_with("-") => exprs.push(query::image::not(query::image::tag(
                    negate.strip_prefix("-").unwrap(),
                ))),
                order if tag.starts_with("order:") => match order.strip_prefix("order:").unwrap() {
                    "random" => order_by = Some(OrderBy::Random),
                    "created_at" => order_by = Some(OrderBy::CreatedAtAsc),
                    "created_at_desc" => order_by = Some(OrderBy::CreatedAtDesc),
                    "filesize" => order_by = Some(OrderBy::FileSizeAsc),
                    "filesize_desc" => order_by = Some(OrderBy::FileSizeDesc),
                    _ => (),
                },
                other => exprs.push(query::image::tag(other)),
            }
        }

        query::ImageQuery {
            expr: exprs
                .into_iter()
                .reduce(ImageQueryExpr::and)
                .map(ImageQueryKind::Where)
                .unwrap_or(ImageQueryKind::All),
            limit: value.limit.or(Some(20)),
            offset: Some(
                value
                    .page
                    .unwrap_or(1)
                    .saturating_sub(1)
                    * value.limit.unwrap_or(20),
            ),
            order: order_by.or(Some(OrderBy::CreatedAtDesc)),
        }
    }
}

pub async fn get_images(
    State(app): State<AppState>,
    Query(params): Query<ImageQueryParam>,
) -> Result<Json<Vec<ImageResponse>>, ImageError> {
    let results = query_image(&app.db, &app.storage, params.into()).await?;

    Ok(Json(
        results
            .into_iter()
            .map(|image| ImageResponse::from_image(app.config.clone(), image))
            .collect(),
    ))
}

pub async fn get_image(
    State(app): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<ImageResponse>, ImageError> {
    let hash = PixelHash::from_signed(id);

    let image = find_image_by_hash(&app.db, &app.storage, &hash).await?;

    Ok(Json(ImageResponse::from_image(app.config, image)))
}

pub async fn post_image(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<ImageResponse>, ImageError> {
    let mut bytes = None;
    let mut tags = vec![];
    let mut source = None;

    while let Some(field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or_default().to_string();

        match name.as_str() {
            "file" => {
                let mut data = BytesMut::new();
                let mut stream = field.into_stream();
                while let Some(chunk) = stream.try_next().await.unwrap_or(None) {
                    data.extend_from_slice(&chunk);
                }
                bytes = Some(data.freeze().to_vec());
            }
            "tags" => {
                let text = field.text().await.unwrap_or_default();
                tags = text.split_whitespace().map(str::to_string).collect();
            }
            "source" => {
                source = Some(field.text().await.unwrap_or_default());
            }
            _ => {} // ignore
        }
    }

    let bytes = match bytes {
        Some(b) => b,
        None => return Err(ImageError::BadRequest("missing file".to_string())),
    };

    let img = ArchiveImageCommand {
        bytes,
        tags,
        source,
    }
    .execute(&state.storage, &state.db)
    .await?;

    Ok(Json(ImageResponse::from_image(state.config, img)))
}

pub async fn put_tags(
    State(app): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<ImageQueryParam>,
) -> Result<Json<ImageResponse>, ImageError> {
    let tags = params.tags.unwrap_or_default();
    let tags = tags.split_whitespace().collect::<Vec<_>>();
    let hash = PixelHash::from_signed(id);

    attach_tags(&app.db, &app.storage, &hash, &tags).await?;

    Ok(Json(ImageResponse::from_image(
        app.config,
        find_image_by_hash(&app.db, &app.storage, &hash).await?,
    )))
}

pub async fn delete_image(
    State(app): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ImageError> {
    let hash = PixelHash::from_signed(id);

    remove_image(&app.storage, &app.db, hash).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub enum ImageError {
    App(AppError),

    BadRequest(String),
}

impl From<AppError> for ImageError {
    fn from(value: AppError) -> Self {
        ImageError::App(value)
    }
}

impl IntoResponse for ImageError {
    fn into_response(self) -> axum::response::Response {
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        let (status, message) = match self {
            ImageError::App(app_error) => match app_error {
                AppError::Storage(storage_error) => match storage_error {
                    StorageError::HashCollision { hash, .. } => {
                        (StatusCode::BAD_REQUEST, hash.to_string())
                    }
                    StorageError::UnsupportedFile { kind } => (
                        StatusCode::BAD_REQUEST,
                        kind.map(|k| k.mime_type().to_string())
                            .unwrap_or("unknown".to_string()),
                    ),
                    StorageError::FileNotFound { hash } => {
                        (StatusCode::NOT_FOUND, hash.to_string())
                    }
                    StorageError::Io(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
                    StorageError::Image(image_error) => {
                        (StatusCode::UNPROCESSABLE_ENTITY, image_error.to_string())
                    }
                    StorageError::Video(error) => {
                        (StatusCode::UNPROCESSABLE_ENTITY, error.to_string())
                    }
                    StorageError::Thumbnail { reason } => {
                        (StatusCode::UNPROCESSABLE_ENTITY, reason)
                    }
                },
                AppError::Database(database_error) => {
                    (StatusCode::SERVICE_UNAVAILABLE, database_error.to_string())
                }
                AppError::StorageNotFound { hash } => (StatusCode::NOT_FOUND, hash.to_string()),
            },
            ImageError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };

        (status, Json(ErrorResponse { message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::ImageQueryParam;
    use buru::query::{ImageQuery, ImageQueryKind, OrderBy, image};

    #[test]
    fn test_build_query() {
        let image_query = ImageQueryParam {
            tags: Some("cat cute -black order:random".to_string()),
            page: None,
            limit: None,
        };

        assert_eq!(
            ImageQuery {
                expr: ImageQueryKind::Where(
                    image::tag("cat")
                        .and(image::tag("cute"))
                        .and(image::not(image::tag("black")))
                ),
                limit: Some(20),
                offset: Some(0),
                order: Some(OrderBy::Random)
            },
            image_query.into()
        )
    }
}
