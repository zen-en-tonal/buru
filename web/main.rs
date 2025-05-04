mod post;

use axum::{Router, routing::get};
use buru::{database::Database, storage::Storage};
use post::AppState;
use sqlx::{Pool, Sqlite, migrate::MigrateDatabase};
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    Sqlite::create_database("sqlite:./db/database.db")
        .await
        .unwrap();

    let db = Database::with_migration(Pool::connect("sqlite:./db/database.db").await.unwrap())
        .await
        .unwrap();
    let storage = Storage::new(PathBuf::from("./images"));

    let state = AppState {
        db: Arc::new(db),
        storage: Arc::new(storage),
    };

    let app = Router::new()
        .route("/posts.json", get(post::get_posts))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
