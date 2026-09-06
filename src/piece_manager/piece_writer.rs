use std::{io::SeekFrom, path::PathBuf};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{torrent_parser::metadata::File as TorrentFile, wire_protocol::Bitfield};

#[async_trait::async_trait]
pub trait PieceWriter {
    type Error;

    async fn initialize(&mut self, bitfield_length: u32) -> Result<Option<Bitfield>, Self::Error>;
    async fn read(
        &self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error>;
    async fn write(
        &mut self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error>;
    async fn set_bitfield(&mut self, bitfield: Bitfield) -> Result<(), Self::Error>;
    async fn finalize(&mut self) -> Result<(), Self::Error>;
}

pub struct DiskPieceWriter {
    pub temp_file: PathBuf,
    pub total_length: u64,
    pub name: String,
    pub md5sum: Option<String>,
    pub files: Option<Vec<TorrentFile>>,
    pub info_hash: [u8; 20],
}

impl DiskPieceWriter {
    pub fn new(
        total_length: u64,
        name: &String,
        md5sum: &Option<String>,
        files: &Option<Vec<TorrentFile>>,
        info_hash: [u8; 20],
    ) -> Self {
        let temp_file = std::env::current_dir()
            .unwrap()
            .join(format!("{name}.dhaar"));
        Self {
            temp_file,
            total_length,
            name: name.clone(),
            md5sum: md5sum.clone(),
            files: files.clone(),
            info_hash,
        }
    }
}

#[async_trait::async_trait]
impl PieceWriter for DiskPieceWriter {
    type Error = std::io::Error;
    async fn initialize(&mut self, bitfield_length: u32) -> Result<Option<Bitfield>, Self::Error> {
        let file_length = self.total_length + bitfield_length as u64 + self.info_hash.len() as u64;

        if !self.temp_file.exists() {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.temp_file)
                .await?;
            if file.metadata().await?.len() != file_length {
                file.set_len(file_length).await?;
            }
            file.seek(SeekFrom::Start(self.total_length)).await?;
            file.write_all(&vec![0u8; bitfield_length as usize]).await?;
            file.seek(SeekFrom::End(-(self.info_hash.len() as i64)))
                .await?;
            file.write_all(&self.info_hash).await?;
            file.sync_all().await?;
            return Ok(None);
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.temp_file)
            .await?;
        let metadata = file.metadata().await?;
        if metadata.len() != file_length {
            file.set_len(file_length).await?;
            file.seek(SeekFrom::Start(self.total_length)).await?;
            file.write_all(&vec![0u8; bitfield_length as usize]).await?;
            file.seek(SeekFrom::End(-(self.info_hash.len() as i64)))
                .await?;
            file.write_all(&self.info_hash).await?;
            file.sync_all().await?;
            return Ok(None);
        }

        file.seek(SeekFrom::End(-(self.info_hash.len() as i64)))
            .await?;
        let mut info_hash_from_file = vec![0u8; self.info_hash.len()];
        file.read_exact(&mut info_hash_from_file).await?;
        if info_hash_from_file != self.info_hash {
            return Ok(None);
        }

        let bitfield_offset = self.total_length;
        let mut bitfield_from_file = vec![0u8; bitfield_length as usize];
        file.seek(SeekFrom::Start(bitfield_offset)).await?;
        file.read_exact(&mut bitfield_from_file).await?;
        let bitfield = Some(Bitfield(bitfield_from_file));

        Ok(bitfield)
    }
    async fn read(
        &self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error> {
        let mut file = File::open(&self.temp_file).await?;
        file.seek(SeekFrom::Start(
            piece_length * piece_index as u64 + piece_offset,
        ))
        .await?;
        let mut buf = vec![0; length as usize];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }
    async fn write(
        &mut self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let mut file = OpenOptions::new().write(true).open(&self.temp_file).await?;
        file.seek(SeekFrom::Start(
            piece_length * piece_index as u64 + piece_offset,
        ))
        .await?;
        file.write_all(&data).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn set_bitfield(&mut self, bitfield: Bitfield) -> Result<(), Self::Error> {
        let bitfield_offset = self.total_length;
        let mut file = OpenOptions::new().write(true).open(&self.temp_file).await?;
        file.seek(SeekFrom::Start(bitfield_offset as u64)).await?;
        file.write_all(&bitfield.0).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<(), Self::Error> {
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
                tokio::io::copy(&mut src.take(self.total_length), &mut out).await?;
            }
        }

        Ok(())
    }
}
