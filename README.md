# buru

[![docs.rs](https://docs.rs/buru/badge.svg)](https://docs.rs/buru)

`buru` is a Rust application that lets you archive and tag images either from the command line or through a small web API.  It stores image metadata in a database and exposes convenient tools for managing that data.

## Features

- **CLI** for archiving images and adding tags
- **Web server** exposing a REST API for programmatic access
- **SQLite** database integration (optional PostgreSQL via feature flag)
- **Asynchronous** processing for good runtime performance
- **Docker** configuration for easy deployment

## Quick start

### Requirements

- [Rust](https://www.rust-lang.org/tools/install)
- [Docker](https://www.docker.com/get-started) (optional for container use)

### Build and run

Clone the repository and build the project:

```bash
git clone https://github.com/zen-en-tonal/buru.git
cd buru
cargo build --release
```

Run the CLI to archive images:

```bash
cargo run --bin cli -- archive --path /path/to/image.jpg --tags "nature sunset"
```

Start the web server (listens on port 3000 by default):

```bash
cargo run --bin web
```

### Docker

A `docker-compose.yml` file is provided. To build and start all services:

```bash
docker-compose build
docker-compose up
```

## Web API

The web server exposes a small REST-style API that returns JSON. Endpoints are
listed below using the default port `3000`.

### `GET /images`

List images. Query parameters:

- `tags` &ndash; space separated tag query
- `page` &ndash; page number (default 1)
- `limit` &ndash; results per page (default 20)

### `GET /images/{id}`

Retrieve metadata for a single image by numeric identifier.

### `POST /images`

Upload a new image using `multipart/form-data` with these fields:

- `file` &ndash; binary file contents (required)
- `tags` &ndash; space separated tags (optional)
- `source` &ndash; original source URL (optional)

### `PUT /images/{id}/tags`

Replace all tags for the image identified by `id`. Supply new tags via the
`tags` query parameter (e.g. `?tags=cute+cat`).

### `DELETE /images/{id}`

Remove an image and its metadata.

### `GET /tags`

List tags. Supports the following query parameters:

- `search[name_comma]` &ndash; comma separated tag names to match
- `page` and `limit` &ndash; pagination controls

### `GET /tags/suggest`

Suggest tags by prefix. Use `search[query]` to supply the prefix and `limit` to
cap results.

### `PUT /refresh/tag_counts`

Recompute stored counts for all tags.

### `GET /files/{vari}/{hash}`

Fetch an image file. The `{vari}` segment is one of the generated variants
(`original`, `sample`, `180x180`, etc.) and `{hash}` is the image file path.

## License

This project is licensed under the MIT OR Apache-2.0 license. See the `LICENSE` file for details.

