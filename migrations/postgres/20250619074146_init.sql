-- Add migration script here

CREATE TABLE images (
    hash TEXT PRIMARY KEY,
    source TEXT
);

CREATE TABLE image_metadatas (
    image_hash TEXT PRIMARY KEY,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    format TEXT NOT NULL,
    color_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    duration DOUBLE PRECISION,
    FOREIGN KEY (image_hash) REFERENCES images(hash) ON DELETE CASCADE
);

CREATE INDEX idx_image_metadatas_created_at_desc
ON image_metadatas (created_at DESC);

CREATE TABLE tags (
    name TEXT PRIMARY KEY
);

CREATE TABLE image_tags (
    image_hash TEXT,
    tag_name TEXT,
    PRIMARY KEY (image_hash, tag_name),
    FOREIGN KEY (image_hash) REFERENCES images(hash) ON DELETE CASCADE,
    FOREIGN KEY (tag_name) REFERENCES tags(name) ON DELETE CASCADE
);

CREATE VIEW image_with_metadata AS
SELECT *
FROM images
LEFT JOIN image_metadatas ON images.hash = image_metadatas.image_hash;

CREATE TABLE tag_counts (
    tag_name TEXT PRIMARY KEY,
    count BIGINT NOT NULL,
    FOREIGN KEY (tag_name) REFERENCES tags(name) ON DELETE CASCADE
);
