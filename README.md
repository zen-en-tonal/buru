# buru

[![docs.rs](https://docs.rs/buru/badge.svg)](https://docs.rs/buru)

This project provides a comprehensive system for managing and archiving images, offering a robust CLI and a web interface to efficiently store images, extract metadata, and handle database interactions. The application is built in Rust and utilizes asynchronous processing to ensure high performance and scalability.

## Overview

The system consists of two main components:
- **CLI Application**: Enables users to archive images via command line, adding metadata and tags.
- **Web Interface**: Provides a RESTful API for interacting with the archival system programmatically.

## Features

- **Image Storage**: Efficiently handles image storage with metadata extraction and tagging capabilities.
- **Database Integration**: Uses SQLite for storing image metadata, tags, and source information.
- **Tagging System**: Supports tagging images to facilitate easy searching and categorization.
- **Asynchronous Execution**: Leverages async features of Rust for non-blocking operations.
- **Docker Support**: Containerization of the application with Docker for easy deployment.

## Setup

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) for building the project.
- [Docker](https://www.docker.com/get-started) for running the application in containers.
- [SQLite](https://www.sqlite.org/download.html) database for metadata storage.

### Installation

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd image-archival-system
   ```

2. **Build the project:**

   ```bash
   cargo build --release
   ```

3. **Run the CLI or Web Server:**

   - **CLI**: Execute image archival commands.
     ```bash
     cargo run --bin cli
     ```

   - **Web Server**: Start the RESTful API.
     ```bash
     cargo run --bin web
     ```

4. **Docker**: Optionally, build and run using Docker.

   - Build Docker Image:

     ```bash
     docker-compose build
     ```

   - Run with Docker Compose:

     ```bash
     docker-compose up
     ```

## Usage

### CLI Application

To archive an image using the CLI:

```bash
cargo run --bin cli -- archive --path /path/to/image.jpg --tags "nature sunset" --source "http://example.com/source"
```

### Web Interface

The web interface provides endpoints to archive images, retrieve metadata, and manage tags. Start the server and access the API documentation at `[localhost:3000]`.

#### Example

- **GET /images**: Fetch all archived images.
- **POST /images**: Upload and archive a new image.

### Docker Deployment

The application can be easily deployed using Docker. Use the provided `docker-compose.yml` to manage services. This setup includes both the application server and a reverse proxy using Nginx.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
