use std::{io::SeekFrom, path::PathBuf};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::torrent_parser::metadata::File as TorrentFile;

#[async_trait::async_trait]
pub trait PieceWriter {
    type Error;

    async fn initialize(&self) -> Result<(), Self::Error>;
    async fn read(&self, piece_index: u64, piece_offset: u64) -> Result<Vec<u8>, Self::Error>;
    async fn write(
        &self,
        piece_index: u64,
        piece_offset: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error>;
    async fn finalize(&self) -> Result<(), Self::Error>;
}

pub struct DiskPieceWriter {
    pub temp_file: PathBuf,
    pub piece_length: u64,
    pub name: String,
    pub length: Option<u64>,
    pub md5sum: Option<String>,
    pub files: Option<Vec<TorrentFile>>,
}

impl DiskPieceWriter {
    pub fn new(
        piece_length: u64,
        name: String,
        length: Option<u64>,
        md5sum: Option<String>,
        files: Option<Vec<TorrentFile>>,
    ) -> Self {
        Self {
            temp_file: PathBuf::new(),
            piece_length,
            name,
            length,
            md5sum,
            files,
        }
    }
}

#[async_trait::async_trait]
impl PieceWriter for DiskPieceWriter {
    type Error = std::io::Error;
    async fn initialize(&self) -> Result<(), Self::Error> {
        if !self.temp_file.exists() {
            let file = File::create(&self.temp_file).await?;
            file.set_len(self.piece_length).await?;
        }
        Ok(())
    }
    async fn read(&self, piece_index: u64, piece_offset: u64) -> Result<Vec<u8>, Self::Error> {
        let mut file = File::open(&self.temp_file).await?;
        file.seek(SeekFrom::Start(
            self.piece_length * piece_index + piece_offset,
        ))
        .await?;
        let mut buf = vec![0; self.piece_length as usize];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }
    async fn write(
        &self,
        piece_index: u64,
        piece_offset: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let mut file = OpenOptions::new().write(true).open(&self.temp_file).await?;
        file.seek(SeekFrom::Start(
            self.piece_length * piece_index + piece_offset,
        ))
        .await?;
        file.write_all(&data).await?;
        Ok(())
    }

    async fn finalize(&self) -> Result<(), Self::Error> {
        let base_dir = self
            .temp_file
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut src = File::open(&self.temp_file).await?;

        match &self.files {
            Some(files) => {
                let root = base_dir.join(&self.name);
                for file in files {
                    let mut path = root.clone();
                    path.extend(&file.path);
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let mut out = File::create(&path).await?;
                    let mut limited = (&mut src).take(file.length);
                    tokio::io::copy(&mut limited, &mut out).await?;
                }
            }
            None => {
                tokio::fs::create_dir_all(&base_dir).await?;
                let path = base_dir.join(&self.name);
                let mut out = File::create(&path).await?;
                tokio::io::copy(&mut src, &mut out).await?;
            }
        }

        Ok(())
    }
}
