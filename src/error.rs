use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("toml error: {0}")]
    TomlError(#[from] toml::de::Error),
    #[error("error: {0}")]
    Error(String),
}
