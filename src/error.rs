use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error: {0}")]
    Custom(String),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml de error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("toml ser error: {0}")]
    TomlSer(#[from] toml::ser::Error),
    #[error("bencode error: {0}")]
    Bencode(#[from] bencode::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
