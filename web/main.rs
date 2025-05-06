mod image;
mod tag;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use buru::{database::Database, storage::Storage};
use sqlx::{Pool, Sqlite, migrate::MigrateDatabase};
use std::{env, fs};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub cdn_base_url: PathBuf,
    pub image_dir: PathBuf,
    pub port: u16,
    pub body_limit: usize,
}

impl AppConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        AppConfig {
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL is required"),
            cdn_base_url: env::var("CDN_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:3000/files".to_string())
                .into(),
            image_dir: env::var("IMAGE_DIR")
                .unwrap_or_else(|_| "static/images".to_string())
                .into(),
            port: env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3000),
            body_limit: env::var("BODY_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(20 * 1024 * 1024), // 20 MB
        }
    }

    pub async fn create_database(&self) {
        Sqlite::create_database(&self.database_url).await.unwrap();
    }

    pub async fn into_state(self) -> AppState {
        let db = Database::with_migration(Pool::connect(&self.database_url).await.unwrap())
            .await
            .unwrap();
        let storage = Storage::new(self.image_dir.clone());

        AppState {
            db: Arc::new(db),
            storage: Arc::new(storage),
            config: self,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub storage: Arc<Storage>,
    pub config: AppConfig,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env();
    config.create_database().await;

    let addr = format!("0.0.0.0:{}", config.port);

    let app = Router::new()
        .route("/images", get(image::get_images).post(image::post_image))
        .route("/images/{id}", get(image::get_image))
        .route("/tags", get(tag::get_tags))
        .route("/files/{*hash}", get(serve_file))
        .layer(DefaultBodyLimit::max(config.body_limit))
        .with_state(config.into_state().await);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn serve_file(State(state): State<AppState>, Path(hash): Path<String>) -> impl IntoResponse {
    let path = state.config.image_dir.join(PathBuf::from(hash));

    match fs::read(path) {
        Ok(bytes) => Response::builder().body(bytes.into()).unwrap(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
