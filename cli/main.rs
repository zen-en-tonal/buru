use buru::prelude::*;
use clap::{Parser, Subcommand};
use sqlx::Pool;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mybooru")]
#[command(about = "Danbooru-compatible image archive CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Archive {
        #[arg(help = "Path to image file")]
        path: std::path::PathBuf,

        #[arg(short, long, help = "Tags (space separated)")]
        tags: Option<String>,

        #[arg(short, long, help = "Image source URL")]
        source: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let cli = Cli::parse();

    let db = Database::with_migration(Pool::connect("sqlite:./db/database.db").await.unwrap())
        .await
        .unwrap();
    let storage = Storage::new(PathBuf::from("./images"));

    match cli.command {
        Commands::Archive { path, tags, source } => {
            let bytes = tokio::fs::read(&path)
                .await
                .expect("failed to read image bytes");

            let cmd = ArchiveImageCommand {
                bytes,
                tags: tags
                    .unwrap_or_default()
                    .split_whitespace()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                source,
            };

            let image = cmd.execute(&storage, &db).await?;

            println!("✅ Archived image:");
            println!("{:?}", image);
        }
    }

    Ok(())
}
