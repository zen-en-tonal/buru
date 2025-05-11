//! # Image Archival System
//!
//! This crate provides functionalities for storing, tagging, and managing images,
//! including their metadata and associated source information. The primary functionality
//! is exposed through the `ArchiveImageCommand` struct, which provides a fluent interface
//! for constructing and executing archival operations.
//!
//! ## Features
//!
//! - **Image Archival**: Store images along with their metadata and manage them through
//!   a database.
//! - **Tag Management**: Add and manage tags for images to facilitate better categorization
//!   and retrieval.
//! - **Source Attribution**: Attach source URLs to images for better traceability.
//! - **Asynchronous Execution**: Archive images and their associated data asynchronously
//!   to ensure non-blocking operations.
//!
//! ## Usage
//!
//! The main entry point to the system is the `ArchiveImageCommand` struct, which provides
//! chaining methods to customize and execute image archival operations. Example usage is
//! provided in the tests to illustrate how to create, tag, source, and execute an `ArchiveImageCommand`.
//!
//! ```no_run
//! use buru::app::ArchiveImageCommand;
//! use buru::storage::Storage;
//! use buru::database::Database;
//!
//! async fn perform_archival(storage: &Storage, db: &Database, bytes: &[u8]) {
//!     let command = ArchiveImageCommand::new(bytes)
//!         .with_tags(vec!["nature".to_string(), "sunset".to_string()])
//!         .with_source("https://example.com/sunset");
//!
//!     match command.execute(storage, db).await {
//!         Ok(image) => println!("Successfully archived image with hash: {}", image.hash),
//!         Err(error) => eprintln!("Failed to archive image: {}", error),
//!     }
//! }
//! ```
//!
//! This demonstrates how to initialize an `ArchiveImageCommand`, add tags and a source, and
//! execute the archival process asynchronously.
//!

pub mod app;
pub mod database;
mod dialect;
pub mod parser;
pub mod query;
pub mod storage;
