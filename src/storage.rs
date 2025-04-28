use glob::glob;
use image::{DynamicImage, ImageFormat, ImageReader};
use infer;
use md5;
use std::{
    error::Error,
    fmt::Display,
    fs::{self},
    path::PathBuf,
};

/// Main structure to manage file storage based on visual hash.
pub struct Storage {
    root_path: PathBuf,
}

impl Storage {
    pub fn new(root: PathBuf) -> Storage {
        Storage { root_path: root }
    }

    /// Creates a new file entry in the storage.
    ///
    /// This method processes an input byte array as an image, computes a visual hash,
    /// and stores the image in a structured directory hierarchy based on the hash.
    /// If a file with the same visual hash already exists, an error is returned.
    pub fn create_file(&self, bytes: &[u8]) -> Result<Md5Hash, StorageError> {
        // Since `DynamicImage` does not expose the format it was decoded from,
        // we independently guess the file format here based on the byte content.
        // If the format cannot be reliably guessed, the file is considered suspicious
        // and the process is aborted early.
        let kind = infer::get(bytes).ok_or(StorageError::UnsupportedFile)?;

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
        if self.find_entry(&pixel_hash).is_some() {
            return Err(StorageError::HashCollision);
        }

        // Compose the filename as `{pixel_hash}.{extension}`,
        // and save the image using the guessed file format.
        let filename = self.derive_filename(&pixel_hash, kind.extension());
        let filepath = dir_path.join(filename);
        let format =
            ImageFormat::from_extension(kind.extension()).ok_or(StorageError::UnsupportedFile)?;
        img.save_with_format(filepath, format)?;

        Ok(pixel_hash)
    }

    /// Given a hash, returns the relative file path if it exists.
    pub fn index_file(&self, hash: &Md5Hash) -> Option<String> {
        self.find_entry(hash).map(|p| {
            format!(
                "{}{}",
                self.derive_dir(hash),
                &p.file_name()
                    .expect("Failed to get file name")
                    .to_string_lossy()
            )
        })
    }

    /// Derives a relative directory path from the hash (for indexing).
    /// Example: `/01/23/`
    fn derive_dir(&self, hash: &Md5Hash) -> String {
        format!(
            "/{:02x}{:02x}/{:02x}{:02x}/",
            hash.0[0], hash.0[1], hash.0[2], hash.0[3]
        )
    }

    /// Derives the absolute directory path on the filesystem.
    fn derive_abs_dir(&self, hash: &Md5Hash) -> PathBuf {
        self.root_path.join(self.derive_dir(hash))
    }

    /// Generates a filename based on the hash and extension.
    fn derive_filename(&self, hash: &Md5Hash, ext: &str) -> String {
        let hash_str: String = hash.clone().into();

        format!("{}.{}", hash_str, ext.to_string())
    }

    /// Searches for a file matching the hash (with any extension).
    fn find_entry(&self, hash: &Md5Hash) -> Option<PathBuf> {
        let dir = self.derive_abs_dir(hash);
        let filename: String = hash.clone().into();

        let glob_pattern = format!("{}.*", dir.join(filename).to_string_lossy());

        for entry in glob(&glob_pattern).expect("Failed to read glob pattern") {
            return Some(entry.expect("Failed to read entry"));
        }
        return None;
    }
}

/// Errors that can occur during storage operations.
#[derive(Debug)]
pub enum StorageError {
    /// Same pixel hash already exists.
    HashCollision,
    /// File format could not be determined or is unsupported.
    UnsupportedFile,
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

/// Formats StorageError for display (to be implemented).
impl Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Enables StorageError to be used as a standard error type.
impl Error for StorageError {}

/// Represents a 16-byte MD5 hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Md5Hash([u8; 16]);

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
