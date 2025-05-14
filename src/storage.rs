//! Storage module to manage file storage based on pixel hashes.
//!
//! This module provides a storage mechanism for image files, utilizing a pixel-based
//! hash to detect and prevent duplicate visual content storage, regardless of
//! file format differences.
//!
//! The storage system organizes files under a directory tree structure that is
//! derived from the computed pixel hash, improving file system indexing and retrieval.
//! Various errors related to file handling, hash computation, and image processing
//! are managed via the `StorageError` enum, aiding in detailed error reporting.
//!
//! The module includes operations for storing images, managing duplicate detection,
//! retrieving file metadata, and ensuring files are correctly indexed or deleted
//! from the storage system.

pub use chrono::{DateTime, Utc};
use glob::glob;
use image::{DynamicImage, GenericImageView, ImageBuffer, ImageFormat, ImageReader};
use std::hash::Hasher;
use std::{
    fmt::Display,
    fs::{self},
    path::PathBuf,
};
use tempfile::NamedTempFile;
use thiserror::Error;
use twox_hash::XxHash64;
use video_rs::{Decoder, Frame};

#[derive(Debug, Clone)]
pub struct Storage {
    root_path: PathBuf,
}

impl Storage {
    /// Creates a new `Storage` instance with the specified root path.
    ///
    /// # Arguments
    /// * `root` - Root directory path where all files will be stored.
    pub fn new(root: PathBuf) -> Storage {
        Storage { root_path: root }
    }

    /// Creates and saves a new file into storage.
    ///
    /// The file is decoded as an image, and a pixel-based hash is computed.
    /// If another file with the same visual content already exists, an error is returned.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The raw byte array of the image file.
    ///
    /// # Returns
    /// * `Ok(Md5Hash)` - The computed pixel hash if the file was saved successfully.
    /// * `Err(StorageError)` - If there was a collision or a saving error.
    ///
    /// # Errors
    /// - `StorageError::HashCollision` if a file with the same pixel hash already exists.
    /// - `StorageError::UnsupportedFile` if the file type cannot be determined.
    /// - `StorageError::Io` if directory creation or file writing fails.
    /// - `StorageError::Image` if operate the image fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use buru::storage::Storage;
    /// # use tempfile::TempDir;
    /// let storage = Storage::new(TempDir::new().unwrap().path().to_path_buf());
    /// let bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
    /// let hash = storage.create_file(bytes).unwrap();
    /// println!("File stored with pixel hash: {:?}", hash);
    /// ```
    pub fn create_file(&self, bytes: &[u8]) -> Result<PixelHash, StorageError> {
        let media = Media::new(bytes)?;

        // Compute an MD5 hash based on the image pixel data (RGBA).
        // This ensures that the file is uniquely identified by its visual content,
        // not its encoding or metadata differences.
        let pixel_hash = match media {
            Media::Video { ref thumbnail, .. } => compute_pixel_hash(thumbnail),
            Media::Image {
                content: ref reader,
                ..
            } => compute_pixel_hash(reader),
        };

        // Based on the hash value, create a nested directory structure to improve file system indexing.
        // Example path: `/root_dir/12/34/1234567890abcdef1234567890abcdef.png`
        let dir_path = self.derive_abs_dir(&pixel_hash);
        fs::create_dir_all(dir_path.clone())?;

        // If a file with the same pixel hash already exists in the storage,
        // return a collision error to prevent overwriting visually identical content.
        if let Some(entry) = self.find_entry(&pixel_hash) {
            return Err(StorageError::HashCollision {
                existing_path: entry.content_path().to_owned(),
            });
        }

        // Compose the filename as `{pixel_hash}.{extension}`,
        // and save the image using the guessed file format.
        match media {
            Media::Video {
                raw,
                thumbnail,
                kind,
            } => {
                let thumb_filename = self.derive_filename(&pixel_hash, "png");
                let thumb_filepath = dir_path.join(thumb_filename);
                thumbnail.save_with_format(thumb_filepath, ImageFormat::Png)?;

                let video_filename = self.derive_filename(&pixel_hash, kind.extension());
                let video_filepath = dir_path.join(video_filename);
                fs::write(video_filepath, raw)?;
            }
            Media::Image { content, kind } => {
                let filename = self.derive_filename(&pixel_hash, kind.extension());
                let filepath = dir_path.join(filename);
                let format = ImageFormat::from_extension(kind.extension())
                    .ok_or(StorageError::UnsupportedFile { kind: Some(kind) })?;
                content.save_with_format(filepath, format)?;
            }
        }

        Ok(pixel_hash)
    }

    /// Returns the relative path of a stored file based on its hash, if it exists.
    ///
    /// # Arguments
    /// * `hash` - The pixel hash to locate.
    ///
    /// # Returns
    /// * `Some(relative_path)` if the file exists.
    /// * `None` if no matching file is found.
    pub fn index_file(&self, hash: &PixelHash) -> Option<MediaPath> {
        self.find_entry(hash).map(|p| match p {
            MediaPath::Image(path_buf) => MediaPath::Image(
                self.derive_dir(hash)
                    .join(path_buf.file_name().expect("Failed to get file name")),
            ),
            MediaPath::Video { video, thumb } => MediaPath::Video {
                video: self
                    .derive_dir(hash)
                    .join(video.file_name().expect("Failed to get file name")),
                thumb: self
                    .derive_dir(hash)
                    .join(thumb.file_name().expect("Failed to get file name")),
            },
        })
    }

    /// Ensures that the file associated with the given pixel hash does not exist.
    ///
    /// If the file exists, it is deleted.
    /// If the file does not exist, this function still succeeds.
    ///
    /// # Arguments
    /// * `hash` - The pixel hash of the file to delete.
    ///
    /// # Returns
    /// * `Ok(())` if the file does not exist after the call.
    /// * `Err(StorageError::FilesystemError)` if an unexpected I/O error occurs.
    pub fn ensure_deleted(&self, hash: &PixelHash) -> Result<(), StorageError> {
        if let Some(path) = self.find_entry(hash) {
            match path {
                MediaPath::Image(path_buf) => fs::remove_file(path_buf)?,
                MediaPath::Video { video, thumb } => {
                    fs::remove_file(video)?;
                    fs::remove_file(thumb)?;
                }
            }
        }
        Ok(())
    }

    /// Retrieves metadata for an image file associated with a given pixel hash.
    ///
    /// This function attempts to locate the image file corresponding to the provided
    /// pixel hash within the storage system. If found, it reads the file and extracts
    /// detailed metadata about the image, such as its dimensions, format, color type,
    /// file size, and filesystem creation timestamp.
    ///
    /// # Arguments
    /// * `hash` - A reference to the `PixelHash` identifying the image file.
    ///
    /// # Returns
    /// * `Ok(ImageMetadata)` - An `ImageMetadata` struct containing the extracted
    ///   metadata if the file is found and successfully read.
    /// * `Err(StorageError)` - If the file is not found, the file format is
    ///   unsupported, or any I/O or image processing error occurs.
    ///
    /// # Errors
    /// - `StorageError::FileNotFound` if no file is located for the given hash.
    /// - `StorageError::UnsupportedFile` if the file format cannot be determined.
    /// - `StorageError::Io` for any I/O-related errors.
    /// - `StorageError::Image` for any image decoding errors.
    pub fn get_metadata(&self, hash: &PixelHash) -> Result<ImageMetadata, StorageError> {
        let entry = self
            .find_entry(hash)
            .ok_or(StorageError::FileNotFound { hash: hash.clone() })?;
        let file_path = match &entry {
            MediaPath::Image(path_buf) => path_buf,
            MediaPath::Video { thumb, .. } => thumb,
        };

        let bytes = std::fs::read(file_path)?;
        let extension = match &entry {
            MediaPath::Image(path_buf) => path_buf.extension(),
            MediaPath::Video { video, .. } => video.extension(),
        }
        .expect("filepath must have a extention");

        let img = image::load_from_memory(&bytes)?;
        let (width, height) = img.dimensions();
        let color_type = format!("{:?}", img.color());

        let metadata = std::fs::metadata(file_path)?;
        let created_at = metadata.created().map(DateTime::from).ok();
        let file_size = metadata.len();

        let duration = match &entry {
            MediaPath::Image(_) => None,
            MediaPath::Video { video, .. } => {
                Some(Decoder::new(video.as_path())?.duration()?.as_secs_f64())
            }
        };

        Ok(ImageMetadata {
            width,
            height,
            format: extension.to_string_lossy().to_string(),
            color_type,
            file_size,
            created_at,
            duration,
        })
    }

    /// Derives a relative directory path from the hash (for indexing).
    /// Example: `01/23/`
    fn derive_dir(&self, hash: &PixelHash) -> PathBuf {
        PathBuf::from(format!("{:02x}/{:02x}/", hash.0[0], hash.0[1]))
    }

    /// Derives the absolute directory path on the filesystem.
    fn derive_abs_dir(&self, hash: &PixelHash) -> PathBuf {
        self.root_path.join(self.derive_dir(hash))
    }

    /// Generates a filename based on the hash and extension.
    fn derive_filename(&self, hash: &PixelHash, ext: &str) -> PathBuf {
        let hash_str: String = hash.clone().into();

        PathBuf::from(format!("{}.{}", hash_str, ext))
    }

    /// Searches for a file matching the hash (with any extension).
    fn find_entry(&self, hash: &PixelHash) -> Option<MediaPath> {
        let dir = self.derive_abs_dir(hash);
        let filename: String = hash.clone().into();
        let glob_pattern = format!("{}.*", dir.join(filename).to_string_lossy());

        let mut entries: Vec<_> = glob(&glob_pattern).ok()?.filter_map(Result::ok).collect();

        match entries.len() {
            1 => entries.pop().map(MediaPath::Image),
            2 => {
                // .png とそうでない方を振り分ける
                let (a, b) = (entries.pop()?, entries.pop()?);
                let (video, thumb) = match (
                    a.extension().and_then(|e| e.to_str()),
                    b.extension().and_then(|e| e.to_str()),
                ) {
                    (Some("png"), _) => (b, a),
                    (_, Some("png")) => (a, b),
                    _ => return None,
                };

                Some(MediaPath::Video { video, thumb })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Contains metadata about an image stored within the storage system.
///
/// The `ImageMetadata` struct provides detailed information about an image
/// file that has been stored, including its dimensions, format, color type,
/// file size, and the filesystem-based creation timestamp. This metadata is
/// useful for analyzing, displaying, or processing image files based on their
/// characteristics. Having structured metadata aids in efficient storage
/// management and retrieval operations.
///
/// Fields:
/// - `width`: The width of the image in pixels.
/// - `height`: The height of the image in pixels.
/// - `format`: A string representing the file format of the image (e.g., "png").
/// - `color_type`: A string describing the color type or model the image uses
///   (e.g., RGB, Grayscale).
/// - `file_size`: The size of the image file in bytes.
/// - `created_at`: An optional timestamp representing when the file was
///   originally created on the filesystem. It may be `None` if the timestamp
///   is unavailable or unsupported on the platform.
pub struct ImageMetadata {
    pub width: u32,
    pub height: u32,
    pub format: String,
    pub color_type: String,
    pub file_size: u64,

    /// Filesystem-based creation timestamp
    pub created_at: Option<DateTime<Utc>>,

    pub duration: Option<f64>,
}

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Same pixel hash already exists at path: {existing_path}")]
    HashCollision { existing_path: PathBuf },

    #[error("Unsupported or undetectable file format: {kind:?}")]
    UnsupportedFile { kind: Option<infer::Type> },

    #[error("File with pixel hash {hash:?} not found in storage.")]
    FileNotFound { hash: PixelHash },

    #[error("Filesystem I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Image processing error: {0}")]
    Image(#[from] image::ImageError),

    #[error("Video processing error: {0}")]
    Video(#[from] video_rs::Error),

    #[error("Thumbnail generation failure: {reason:}")]
    Thumbnail { reason: String },
}

/// Represents a 8-byte hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PixelHash([u8; 8]);

impl PixelHash {
    #[allow(overflowing_literals)]
    /// Converts the `PixelHash` into a signed 64-bit integer.
    ///
    /// This function takes the `PixelHash` instance and interprets it as
    /// an unsigned 64-bit integer, then converts it to a signed 64-bit
    /// integer using a bitwise XOR with a fixed value. This transformation
    /// ensures that the conversion covers the full range of a signed
    /// 64-bit integer and can be used for comparison and sorting operations.
    ///
    /// # Returns
    /// An `i64` value which represents the signed integer interpretation
    /// of the `PixelHash`.
    pub fn to_signed(self) -> i64 {
        let v: u64 = self.into();
        (v as i64) ^ 0x8000_0000_0000_0000
    }

    #[allow(overflowing_literals)]
    /// Converts a signed 64-bit integer back into a `PixelHash`.
    ///
    /// This function interprets the signed integer as a transformed pixel hash,
    /// reversing the transformation done by `to_signed`. The conversion back to
    /// a `PixelHash` is achieved by applying a bitwise XOR with a fixed value,
    /// effectively reversing the previous transformation.
    ///
    /// This method is useful for reconstructing a `PixelHash` from a signed
    /// integer representation, especially in contexts where `PixelHash` values
    /// are stored or sorted as signed integers.
    ///
    /// # Arguments
    /// * `v` - An `i64` that represents the signed integer interpretation of a
    ///   pixel hash.
    ///
    /// # Returns
    /// A `PixelHash` constructed from the provided signed integer.
    pub fn from_signed(v: i64) -> Self {
        Self::from((v as u64) ^ 0x8000_0000_0000_0000)
    }
}

impl Display for PixelHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .fold("".to_string(), |acc, x| format!("{}{:02x}", acc, x))
        )
    }
}

impl TryFrom<&str> for PixelHash {
    type Error = PixelHashParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl TryFrom<String> for PixelHash {
    type Error = PixelHashParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(PixelHashParseError::InvalidLength);
        }

        let mut bytes = [0u8; 8];

        for (i, byte) in bytes.iter_mut().enumerate() {
            let chunk = &value[i * 2..i * 2 + 2];
            *byte = u8::from_str_radix(chunk, 16).map_err(|_| PixelHashParseError::InvalidHex)?;
        }

        Ok(PixelHash(bytes))
    }
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum PixelHashParseError {
    #[error("hash must be exactly 16 hexadecimal characters.")]
    InvalidLength,

    #[error("hash contains invalid hexadecimal characters.")]
    InvalidHex,
}

/// Converts an Md5Hash into a hex string.
impl From<PixelHash> for String {
    fn from(value: PixelHash) -> Self {
        value.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

impl From<PixelHash> for u64 {
    fn from(value: PixelHash) -> Self {
        u64::from_be_bytes(value.0)
    }
}

impl From<u64> for PixelHash {
    fn from(value: u64) -> Self {
        PixelHash(value.to_be_bytes())
    }
}

impl From<PixelHash> for [u8; 8] {
    fn from(value: PixelHash) -> Self {
        value.0
    }
}

/// Computes a pixel hash from a DynamicImage.
fn compute_pixel_hash(img: &DynamicImage) -> PixelHash {
    let pixels = img.to_rgba8().into_raw();
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(&pixels);

    PixelHash::from(hasher.finish())
}

enum Media {
    Video {
        raw: Vec<u8>,
        thumbnail: DynamicImage,
        kind: infer::Type,
    },
    Image {
        content: DynamicImage,
        kind: infer::Type,
    },
}

impl Media {
    pub fn new(bytes: &[u8]) -> Result<Self, StorageError> {
        let kind = infer::get(bytes).ok_or(StorageError::UnsupportedFile { kind: None })?;

        let media = match kind.matcher_type() {
            infer::MatcherType::Image => Media::Image {
                content: ImageReader::new(std::io::Cursor::new(bytes.to_vec()))
                    .with_guessed_format()?
                    .decode()?,
                kind,
            },
            infer::MatcherType::Video => Media::Video {
                raw: bytes.to_vec(),
                thumbnail: generate_thumbnail(bytes)?,
                kind,
            },
            _ => return Err(StorageError::UnsupportedFile { kind: Some(kind) }),
        };

        Ok(media)
    }
}

fn generate_thumbnail(bytes: &[u8]) -> Result<DynamicImage, StorageError> {
    let tmpfile = write_temp_video(bytes)?;
    let decoder = Decoder::new(tmpfile.path())?;

    let (width, height) = decoder.size();
    let total_frames = decoder.frames()? as i64;
    let fps = decoder.frame_rate();
    let max_frame_for_thumbnail = (fps * 3.0) as i64; // 3 sec

    let target_frame = (total_frames / 2).min(max_frame_for_thumbnail);

    let frame = safe_seek_and_decode(decoder, target_frame)?;
    let buffer = frame.as_slice().ok_or_else(|| StorageError::Thumbnail {
        reason: "Failed to get RGB buffer from frame".to_string(),
    })?;

    let image = ImageBuffer::<image::Rgb<u8>, _>::from_raw(width, height, buffer.to_vec())
        .ok_or_else(|| StorageError::Thumbnail {
            reason: "Failed to construct image buffer".to_string(),
        })?;

    Ok(DynamicImage::ImageRgb8(image))
}

fn write_temp_video(bytes: &[u8]) -> Result<NamedTempFile, StorageError> {
    let tmpfile = NamedTempFile::new()?;
    fs::write(tmpfile.path(), bytes)?;
    tmpfile.as_file().sync_all()?;
    Ok(tmpfile)
}

fn safe_seek_and_decode(mut decoder: Decoder, frame_index: i64) -> Result<Frame, StorageError> {
    decoder.seek_to_start()?;
    match decoder.seek_to_frame(frame_index) {
        Ok(_) => Ok(decoder.decode()?.1),
        Err(_) => {
            decoder.seek_to_start()?;
            // fallback: skip until frame_index by decode().
            for _ in 0..frame_index {
                decoder.decode()?; // skip
            }
            Ok(decoder.decode()?.1)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MediaPath {
    Image(PathBuf),
    Video { video: PathBuf, thumb: PathBuf },
}

impl MediaPath {
    pub fn content_path(&self) -> &PathBuf {
        match self {
            MediaPath::Image(path_buf) => path_buf,
            MediaPath::Video { video, .. } => video,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{MediaPath, PixelHash, PixelHashParseError, Storage, StorageError};
    use std::{fs, i64, path::PathBuf};
    use tempfile::TempDir;

    use super::generate_thumbnail;

    #[test]
    fn test_md5_parse() {
        assert_eq!(
            Ok(PixelHash([50, 148, 53, 229, 230, 107, 232, 9,])),
            PixelHash::try_from("329435e5e66be809".to_string())
        );
        assert_eq!(
            Err(PixelHashParseError::InvalidLength),
            PixelHash::try_from("329435e5e66b".to_string())
        );
        assert_eq!(
            Err(PixelHashParseError::InvalidHex),
            PixelHash::try_from("Z29435e5e66be809".to_string())
        );
        assert_eq!(
            3644597259979188233_u64,
            u64::from(PixelHash::try_from("329435e5e66be809").unwrap())
        );
        assert_eq!(
            i64::MIN,
            PixelHash::try_from("0000000000000000").unwrap().to_signed()
        );
        assert_eq!(
            i64::MAX,
            PixelHash::try_from("ffffffffffffffff").unwrap().to_signed()
        );
        assert_eq!(
            PixelHash::try_from("0000000000000000").unwrap(),
            PixelHash::from_signed(i64::MIN)
        );
        assert_eq!(
            PixelHash::try_from("ffffffffffffffff").unwrap(),
            PixelHash::from_signed(i64::MAX),
        );
    }

    #[test]
    fn test_pathes() {
        let storage = Storage::new("/root".into());

        assert_eq!(
            PathBuf::from("32/94"),
            storage.derive_dir(&PixelHash::try_from("329435e5e66be809".to_string()).unwrap())
        );

        assert_eq!(
            PathBuf::from("/root/32/94"),
            storage.derive_abs_dir(&PixelHash::try_from("329435e5e66be809".to_string()).unwrap())
        )
    }

    #[test]
    fn test_create_file() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        let expect_path = tmp_dir.path().join("44/a5/44a5b6f94f4f6445.png");

        storage.create_file(file_bytes).unwrap();

        assert!(fs::exists(expect_path).unwrap())
    }

    #[test]
    fn test_create_file_on_duplicated() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        let expect_path = tmp_dir.path().join("44/a5/44a5b6f94f4f6445.png");

        storage.create_file(file_bytes).unwrap();

        let result = storage.create_file(file_bytes);
        let Err(StorageError::HashCollision { existing_path }) = result else {
            panic!("Expected HashCollision error, but got {:?}", result);
        };

        assert_eq!(expect_path, existing_path)
    }

    #[test]
    fn test_index_file() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let image_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        let image_expect_path = MediaPath::Image(PathBuf::from("44/a5/44a5b6f94f4f6445.png"));

        storage.create_file(image_bytes).unwrap();

        assert_eq!(
            Some(image_expect_path),
            storage.index_file(&PixelHash::try_from("44a5b6f94f4f6445".to_string()).unwrap())
        );

        assert_eq!(
            None,
            storage.index_file(&PixelHash::try_from("00a5b6f94f4f6445".to_string()).unwrap())
        );

        let video_bytes = include_bytes!("../testdata/motion_video.mp4");
        let video_expect_path = MediaPath::Video {
            video: PathBuf::from("06/a5/06a5e19afdf4c2e3.mp4"),
            thumb: PathBuf::from("06/a5/06a5e19afdf4c2e3.png"),
        };

        storage.create_file(video_bytes).unwrap();

        assert_eq!(
            Some(video_expect_path),
            storage.index_file(&PixelHash::try_from("06a5e19afdf4c2e3".to_string()).unwrap())
        );
    }

    #[test]
    fn test_ensure_deleted() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        storage.create_file(file_bytes).unwrap();

        assert!(
            storage
                .ensure_deleted(&PixelHash::try_from("44a5b6f94f4f6445".to_string()).unwrap())
                .is_ok()
        );

        assert!(
            storage
                .ensure_deleted(&PixelHash::try_from("44a5b6f94f4f6445".to_string()).unwrap())
                .is_ok()
        );

        assert!(
            storage
                .ensure_deleted(&PixelHash::try_from("00a5b6f94f4f6445".to_string()).unwrap())
                .is_ok()
        );
    }

    #[test]
    fn test_get_metadata() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        let hash = storage.create_file(file_bytes).unwrap();

        println!("{:?}", storage.get_metadata(&hash));
    }

    #[test]
    fn test_get_video_metadata() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let video_bytes = include_bytes!("../testdata/motion_video.mp4");

        let hash = storage.create_file(video_bytes).unwrap();

        assert_eq!(Some(3.0), storage.get_metadata(&hash).unwrap().duration);
    }

    #[test]
    fn test_thumbnail() {
        let file_bytes = include_bytes!("../testdata/motion_video.mp4");

        generate_thumbnail(file_bytes).unwrap();
    }
}
