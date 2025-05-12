# Image Archival System

This application is designed to manage image storage, metadata extraction, database integration, and tagging functionalities. The system provides an asynchronous command interface to efficiently archive images, allowing for the addition of tags and source URLs.

## Features

- **Image Storage**: Handles uploading and storing images using a scalable storage system.
- **Metadata Extraction**: Automatically extracts metadata such as width, height, format, and color type.
- **Database Integration**: Stores image data and metadata in a relational database using SQLite.
- **Tagging System**: Allows for tagging of images to facilitate search and categorization.
- **Source URL Handling**: Supports optional setting of a source URL for each image.

## Setup

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [SQLite](https://www.sqlite.org/download.html)

### Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   ```

2. Navigate to the project directory:

   ```bash
   cd image-archival-system
   ```

3. Build the project:

   ```bash
   cargo build
   ```

### Configuration

Ensure that your database is set up by running the necessary migrations:

```bash
sqlite3 database.db < migrations.sql
```

The migrations are defined in `src/dialect/sqlite.rs`.

## Usage

### Archiving an Image

You can create an `ArchiveImageCommand` and execute it to store an image along with its metadata and tags:

```rust
let image_bytes: &[u8] = //... get image bytes
let tags = vec!["nature".to_string(), "sunset".to_string()];

let command = ArchiveImageCommand::new(image_bytes)
    .with_tags(tags)
    .with_source("http://example.com");

let storage = //... obtain storage reference
let db = //... obtain database reference

command.execute(storage, db).await?;
```

### Querying Images

Use the `ImageQuery` system to retrieve images based on tags or other criteria:

```rust
let query = ImageQuery::new(ImageQueryExpr::tag("nature"))
    .with_limit(10)
    .with_offset(0);

let (sql, params) = query.to_sql();
// Execute the query using your database connection
```

## Contributing

Contributions are welcome! Please submit a pull request with a clear description of your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
