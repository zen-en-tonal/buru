//! Storage module to manage file storage based on pixel hashes.
//!
//! Files are stored under a directory tree derived from the pixel hash.
//! Duplicate visual content (regardless of file format) is detected and rejected.

pub use chrono::{DateTime, Utc};
use glob::glob;
use image::{DynamicImage, GenericImageView, ImageFormat, ImageReader};
use std::hash::Hasher;
use std::{
    error::Error,
    fmt::Display,
    fs::{self},
    path::PathBuf,
};
use twox_hash::XxHash64;

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
        // Since `DynamicImage` does not expose the format it was decoded from,
        // we independently guess the file format here based on the byte content.
        // If the format cannot be reliably guessed, the file is considered suspicious
        // and the process is aborted early.
        let kind = infer::get(bytes).ok_or(StorageError::UnsupportedFile { kind: None })?;

        // Decode the byte array into a DynamicImage for further pixel processing.
        let img = ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()?
            .decode()?;

        // Compute an MD5 hash based on the image pixel data (RGBA).
        // This ensures that the file is uniquely identified by its visual content,
        // not its encoding or metadata differences.
        let pixel_hash = compute_pixel_hash(&img);

        // Based on the hash value, create a nested directory structure to improve file system indexing.
        // Example path: `/root_dir/12/34/1234567890abcdef1234567890abcdef.png`
        let dir_path = self.derive_abs_dir(&pixel_hash);
        fs::create_dir_all(dir_path.clone())?;

        // If a file with the same pixel hash already exists in the storage,
        // return a collision error to prevent overwriting visually identical content.
        if let Some(entry) = self.find_entry(&pixel_hash) {
            return Err(StorageError::HashCollision {
                existing_path: entry,
            });
        }

        // Compose the filename as `{pixel_hash}.{extension}`,
        // and save the image using the guessed file format.
        let filename = self.derive_filename(&pixel_hash, kind.extension());
        let filepath = dir_path.join(filename);
        let format = ImageFormat::from_extension(kind.extension())
            .ok_or(StorageError::UnsupportedFile { kind: Some(kind) })?;
        img.save_with_format(filepath, format)?;

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
    pub fn index_file(&self, hash: &PixelHash) -> Option<PathBuf> {
        self.find_entry(hash).map(|p| {
            self.derive_dir(hash)
                .join(p.file_name().expect("Failed to get file name"))
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
            fs::remove_file(path)?;
        }
        Ok(())
    }

    pub fn get_metadata(&self, hash: &PixelHash) -> Result<ImageMetadata, StorageError> {
        let file_path = self
            .find_entry(hash)
            .ok_or(StorageError::FileNotFound { hash: hash.clone() })?;

        let bytes = std::fs::read(&file_path)?;
        let format = infer::get(&bytes).ok_or(StorageError::UnsupportedFile { kind: None })?;

        let img = image::load_from_memory(&bytes)?;
        let (width, height) = img.dimensions();
        let color_type = format!("{:?}", img.color());

        let metadata = std::fs::metadata(&file_path)?;
        let created_at = metadata.created().map(DateTime::from).ok();
        let file_size = metadata.len();

        Ok(ImageMetadata {
            width,
            height,
            format: format.extension().to_string(),
            color_type,
            file_size,
            created_at,
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
    fn find_entry(&self, hash: &PixelHash) -> Option<PathBuf> {
        let dir = self.derive_abs_dir(hash);
        let filename: String = hash.clone().into();

        let glob_pattern = format!("{}.*", dir.join(filename).to_string_lossy());

        for entry in glob(&glob_pattern).expect("Failed to read glob pattern") {
            return Some(entry.expect("Failed to read entry"));
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImageMetadata {
    pub width: u32,
    pub height: u32,
    pub format: String,
    pub color_type: String,
    pub file_size: u64,

    /// Filesystem-based creation timestamp
    pub created_at: Option<DateTime<Utc>>,
}

/// Errors that can occur during storage operations.
#[derive(Debug)]
pub enum StorageError {
    /// Same pixel hash already exists.
    HashCollision {
        existing_path: PathBuf,
    },
    /// File format could not be determined or is unsupported.
    UnsupportedFile {
        kind: Option<infer::Type>,
    },
    FileNotFound {
        hash: PixelHash,
    },
    /// Filesystem IO error.
    Io(std::io::Error),
    /// Image decoding or saving error.
    Image(image::ImageError),
}

/// Allows automatic conversion from std::io::Error.
impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        StorageError::Io(value)
    }
}

/// Allows automatic conversion from image::ImageError.
impl From<image::ImageError> for StorageError {
    fn from(value: image::ImageError) -> Self {
        StorageError::Image(value)
    }
}

/// Formats StorageError for display.
impl Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::HashCollision { existing_path } => {
                write!(
                    f,
                    "Hash collision detected. Existing file at: {}",
                    existing_path.display()
                )
            }
            StorageError::UnsupportedFile { kind } => {
                if let Some(mime) = kind {
                    write!(f, "Unsupported file format: {}", mime)
                } else {
                    write!(f, "Unsupported or unrecognized file format.")
                }
            }
            StorageError::Io(inner) => {
                write!(f, "Filesystem error: {}", inner)
            }
            StorageError::Image(inner) => {
                write!(f, "Image error: {}", inner)
            }
            StorageError::FileNotFound { hash } => write!(f, "File not found: {}", hash),
        }
    }
}

/// Enables StorageError to be used as a standard error type.
impl Error for StorageError {}

/// Represents a 16-byte MD5 hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PixelHash([u8; 8]);

impl PixelHash {
    pub fn to_string(self) -> String {
        self.into()
    }
}

impl Display for PixelHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.clone().to_string())
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

#[derive(Debug, PartialEq, Eq)]
pub enum PixelHashParseError {
    InvalidLength,
    InvalidHex,
}

impl Display for PixelHashParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PixelHashParseError::InvalidLength => {
                write!(f, "MD5 hash must be exactly 32 hexadecimal characters.")
            }
            PixelHashParseError::InvalidHex => {
                write!(f, "MD5 hash contains invalid hexadecimal characters.")
            }
        }
    }
}

impl Error for PixelHashParseError {}

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

#[cfg(test)]
mod tests {
    use crate::storage::{PixelHash, PixelHashParseError, Storage, StorageError};
    use std::{fs, path::PathBuf};
    use tempfile::TempDir;

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
        )
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

        let file_bytes = include_bytes!("../testdata/44a5b6f94f4f6445.png");
        let expect_path = PathBuf::from("44/a5/44a5b6f94f4f6445.png");

        storage.create_file(file_bytes).unwrap();

        assert_eq!(
            Some(expect_path),
            storage.index_file(&PixelHash::try_from("44a5b6f94f4f6445".to_string()).unwrap())
        );

        assert_eq!(
            None,
            storage.index_file(&PixelHash::try_from("00a5b6f94f4f6445".to_string()).unwrap())
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
}
