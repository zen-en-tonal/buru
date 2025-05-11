//! # Query Module
//!
//! This module provides the querying capabilities for the image archival system,
//! allowing users to create and execute queries on images and tags based on
//! specified criteria. The module is divided into submodules for handling
//! image-specific and tag-specific queries.
//!
//! ## Components
//!
//! - **Image Queries**: Utilize the `ImageQuery`, `ImageQueryExpr`, and `ImageQueryKind`
//!   types for constructing and executing complex queries on images, including logical
//!   operations and filter conditions involving tags and metadata.
//!
//! - **Tag Queries**: Implemented via the `TagQuery`, `TagQueryExpr`, and `TagQueryKind`
//!   types to facilitate searching and filtering tags through varied expressions and patterns.
//!
//! ## Usage
//!
//! The module allows you to build queries that can be converted into SQL expressions
//! and executed against a relational database. Image queries can include conditions
//! based on tags, logical expressions, and temporal constraints, while tag queries
//! support operations such as matching, prefix, and containment searches.
//!
//! ## Examples
//!
//! Creating an image query to retrieve images associated with specific tags and ordered by creation date:
//! ```rust
//! # use buru::query::ImageQuery;
//! # use buru::query::ImageQueryKind;
//! # use buru::query::ImageQueryExpr;
//! # use buru::query::OrderBy;
//! let query = ImageQuery::new(ImageQueryKind::Where(ImageQueryExpr::tag("nature")))
//!     .with_limit(10)
//!     .with_offset(0)
//!     .with_order(OrderBy::CreatedAtAsc);
//! let (sql, params) = query.to_sql();
//! ```
//!
//! Creating a tag query to find all tags starting with a specific prefix:
//! ```rust
//! # use buru::query::TagQuery;
//! # use buru::query::TagQueryKind;
//! # use buru::query::TagQueryExpr;
//! let query = TagQuery::new(TagQueryKind::Where(TagQueryExpr::Prefix("na".to_string())))
//!     .with_limit(5)
//!     .with_offset(0);
//! let (sql, params) = query.to_sql();
//! ```

mod image;
mod tag;

pub use image::{ImageQuery, ImageQueryExpr, ImageQueryKind, OrderBy};
pub use tag::{TagQuery, TagQueryExpr, TagQueryKind};
