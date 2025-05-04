//! Storage module to manage file storage based on pixel hashes.
//!
//! Files are stored under a directory tree derived from the pixel hash.
//! Duplicate visual content (regardless of file format) is detected and rejected.

pub use chrono::{DateTime, Utc};
use glob::glob;
use image::{DynamicImage, GenericImageView, ImageFormat, ImageReader};
use std::{
    error::Error,
    fmt::Display,
    fs::{self},
    path::PathBuf,
};

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
    /// let bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
    /// let hash = storage.create_file(bytes).unwrap();
    /// println!("File stored with pixel hash: {:?}", hash);
    /// ```
    pub fn create_file(&self, bytes: &[u8]) -> Result<Md5Hash, StorageError> {
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
    pub fn index_file(&self, hash: &Md5Hash) -> Option<PathBuf> {
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
    pub fn ensure_deleted(&self, hash: &Md5Hash) -> Result<(), StorageError> {
        if let Some(path) = self.find_entry(hash) {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    pub fn get_metadata(&self, hash: &Md5Hash) -> Result<ImageMetadata, StorageError> {
        let file_path = self
            .find_entry(hash)
            .ok_or(StorageError::FileNotFound { hash: hash.clone() })?;

        let bytes = std::fs::read(&file_path)?;
        let format = infer::get(&bytes).ok_or(StorageError::UnsupportedFile { kind: None })?;

        let img = image::load_from_memory(&bytes)?;
        let (width, height) = img.dimensions();
        let color_type = format!("{:?}", img.color());

        let metadata = std::fs::metadata(&file_path)?;
        let created_at = metadata
            .created()
            .expect("failed to get created_at: filesystem does not support")
            .into();
        let file_size = metadata.len();

        Ok(ImageMetadata {
            width,
            height,
            format: format.mime_type().to_string(),
            color_type,
            file_size,
            created_at,
        })
    }

    /// Derives a relative directory path from the hash (for indexing).
    /// Example: `01/23/`
    fn derive_dir(&self, hash: &Md5Hash) -> PathBuf {
        PathBuf::from(format!("{:02x}/{:02x}/", hash.0[0], hash.0[1]))
    }

    /// Derives the absolute directory path on the filesystem.
    fn derive_abs_dir(&self, hash: &Md5Hash) -> PathBuf {
        self.root_path.join(self.derive_dir(hash))
    }

    /// Generates a filename based on the hash and extension.
    fn derive_filename(&self, hash: &Md5Hash, ext: &str) -> PathBuf {
        let hash_str: String = hash.clone().into();

        PathBuf::from(format!("{}.{}", hash_str, ext))
    }

    /// Searches for a file matching the hash (with any extension).
    fn find_entry(&self, hash: &Md5Hash) -> Option<PathBuf> {
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
    pub created_at: DateTime<Utc>,
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
        hash: Md5Hash,
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
pub struct Md5Hash([u8; 16]);

impl Md5Hash {
    pub fn to_string(self) -> String {
        self.into()
    }
}

impl Display for Md5Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.clone().to_string())
    }
}

impl TryFrom<&str> for Md5Hash {
    type Error = Md5HashParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl TryFrom<String> for Md5Hash {
    type Error = Md5HashParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 32 {
            return Err(Md5HashParseError::InvalidLength);
        }

        let mut bytes = [0u8; 16];

        for (i, byte) in bytes.iter_mut().enumerate() {
            let chunk = &value[i * 2..i * 2 + 2];
            *byte = u8::from_str_radix(chunk, 16).map_err(|_| Md5HashParseError::InvalidHex)?;
        }

        Ok(Md5Hash(bytes))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Md5HashParseError {
    InvalidLength,
    InvalidHex,
}

impl Display for Md5HashParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Md5HashParseError::InvalidLength => {
                write!(f, "MD5 hash must be exactly 32 hexadecimal characters.")
            }
            Md5HashParseError::InvalidHex => {
                write!(f, "MD5 hash contains invalid hexadecimal characters.")
            }
        }
    }
}

impl Error for Md5HashParseError {}

/// Converts an Md5Hash into a hex string.
impl From<Md5Hash> for String {
    fn from(value: Md5Hash) -> Self {
        value.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Computes a pixel hash from a DynamicImage.
fn compute_pixel_hash(img: &DynamicImage) -> Md5Hash {
    let pixels = img.to_rgba8().into_raw();
    let digest = md5::compute(&pixels);

    Md5Hash(digest.0)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::storage::{Md5Hash, Md5HashParseError, Storage, StorageError};
    use tempfile::TempDir;

    #[test]
    fn test_md5_parse() {
        assert_eq!(
            Ok(Md5Hash([
                50, 148, 53, 229, 230, 107, 232, 9, 166, 86, 175, 16, 95, 66, 64, 30
            ])),
            Md5Hash::try_from("329435e5e66be809a656af105f42401e".to_string())
        );
        assert_eq!(
            Err(Md5HashParseError::InvalidLength),
            Md5Hash::try_from("329435e5e66b".to_string())
        );
        assert_eq!(
            Err(Md5HashParseError::InvalidHex),
            Md5Hash::try_from("Z29435e5e66be809a656af105f42401e".to_string())
        );
    }

    #[test]
    fn test_pathes() {
        let storage = Storage::new("/root".into());

        assert_eq!(
            PathBuf::from("ab/cd"),
            storage.derive_dir(
                &Md5Hash::try_from("abcd35e5e66be809a656af105f42401e".to_string()).unwrap()
            )
        );

        assert_eq!(
            PathBuf::from("/root/ab/cd"),
            storage.derive_abs_dir(
                &Md5Hash::try_from("abcd35e5e66be809a656af105f42401e".to_string()).unwrap()
            )
        )
    }

    #[test]
    fn test_create_file() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
        let expect_path = tmp_dir
            .path()
            .join("62/0a/620a139c9d3e63188299d0150c198bd5.png");

        storage.create_file(file_bytes).unwrap();

        assert!(fs::exists(expect_path).unwrap())
    }

    #[test]
    fn test_create_file_on_duplicated() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
        let expect_path = tmp_dir
            .path()
            .join("62/0a/620a139c9d3e63188299d0150c198bd5.png");

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

        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
        let expect_path = PathBuf::from("62/0a/620a139c9d3e63188299d0150c198bd5.png");

        storage.create_file(file_bytes).unwrap();

        assert_eq!(
            Some(expect_path),
            storage.index_file(
                &Md5Hash::try_from("620a139c9d3e63188299d0150c198bd5".to_string()).unwrap()
            )
        );

        assert_eq!(
            None,
            storage.index_file(
                &Md5Hash::try_from("020a139c9d3e63188299d0150c198bd5".to_string()).unwrap()
            )
        );
    }

    #[test]
    fn test_ensure_deleted() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
        storage.create_file(file_bytes).unwrap();

        assert!(
            storage
                .ensure_deleted(
                    &Md5Hash::try_from("620a139c9d3e63188299d0150c198bd5".to_string()).unwrap()
                )
                .is_ok()
        );

        assert!(
            storage
                .ensure_deleted(
                    &Md5Hash::try_from("620a139c9d3e63188299d0150c198bd5".to_string()).unwrap()
                )
                .is_ok()
        );

        assert!(
            storage
                .ensure_deleted(
                    &Md5Hash::try_from("020a139c9d3e63188299d0150c198bd5".to_string()).unwrap()
                )
                .is_ok()
        );
    }

    #[test]
    fn test_get_metadata() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::new(tmp_dir.path().to_path_buf());

        let file_bytes = include_bytes!("../testdata/620a139c9d3e63188299d0150c198bd5.png");
        let hash = storage.create_file(file_bytes).unwrap();

        println!("{:?}", storage.get_metadata(&hash));
    }
}
