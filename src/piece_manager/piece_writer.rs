use std::{os::unix::fs::FileExt, path::PathBuf, sync::Arc};

use tokio::{fs::File, io::AsyncReadExt, task::spawn_blocking};

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

/// The store is one file laid out as `[payload][bitfield][info_hash]`, split
/// into the torrent's real shape only by `finalize`.
pub struct DiskPieceWriter {
    pub temp_file: PathBuf,
    /// Opened once by `initialize` and held for the life of the download.
    /// Every access is positional (`pread`/`pwrite`), so there is no shared
    /// cursor for reads and writes to fight over — which is what lets `read`
    /// keep `&self` while using the same handle `write` does.
    file: Option<Arc<std::fs::File>>,
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
            file: None,
            total_length,
            name: name.clone(),
            md5sum: md5sum.clone(),
            files: files.clone(),
            info_hash,
        }
    }

    fn handle(&self) -> std::io::Result<Arc<std::fs::File>> {
        self.file
            .clone()
            .ok_or_else(|| std::io::Error::other("piece writer used before initialize"))
    }
}

#[async_trait::async_trait]
impl PieceWriter for DiskPieceWriter {
    type Error = std::io::Error;

    async fn initialize(&mut self, bitfield_length: u32) -> Result<Option<Bitfield>, Self::Error> {
        let path = self.temp_file.clone();
        let total_length = self.total_length;
        let info_hash = self.info_hash;
        let bitfield_length = bitfield_length as usize;
        let file_length = total_length + bitfield_length as u64 + info_hash.len() as u64;

        let (file, bitfield) = spawn_blocking(move || {
            let existed = path.exists();
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;

            // A store of the wrong size is not one we can resume from, so it
            // is laid out fresh exactly as a missing one would be.
            if !existed || file.metadata()?.len() != file_length {
                file.set_len(file_length)?;
                file.write_all_at(&vec![0u8; bitfield_length], total_length)?;
                file.write_all_at(&info_hash, file_length - info_hash.len() as u64)?;
                file.sync_all()?;
                return Ok::<_, std::io::Error>((file, None));
            }

            // Right size, but it has to be the same torrent before its
            // bitfield means anything.
            let mut stored = [0u8; 20];
            let stored_at = file_length - stored.len() as u64;
            file.read_exact_at(&mut stored, stored_at)?;
            if stored != info_hash {
                return Ok((file, None));
            }

            let mut bits = vec![0u8; bitfield_length];
            file.read_exact_at(&mut bits, total_length)?;
            Ok((file, Some(Bitfield(bits))))
        })
        .await
        .map_err(std::io::Error::other)??;

        self.file = Some(Arc::new(file));
        Ok(bitfield)
    }

    async fn read(
        &self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error> {
        let file = self.handle()?;
        let offset = piece_length * piece_index as u64 + piece_offset;
        spawn_blocking(move || {
            let mut buf = vec![0; length as usize];
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn write(
        &mut self,
        piece_index: u32,
        piece_offset: u64,
        piece_length: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let file = self.handle()?;
        let offset = piece_length * piece_index as u64 + piece_offset;
        // Deliberately unsynced. Nothing claims a block until the bitfield
        // does, and `set_bitfield` is where that claim is made durable — one
        // barrier per completed piece instead of one per block. A block lost
        // to a crash before then costs a re-download and nothing else.
        spawn_blocking(move || file.write_all_at(&data, offset))
            .await
            .map_err(std::io::Error::other)?
    }

    async fn set_bitfield(&mut self, bitfield: Bitfield) -> Result<(), Self::Error> {
        let file = self.handle()?;
        let offset = self.total_length;
        spawn_blocking(move || {
            // The bitfield asserts a piece is on disk, and resume takes it at
            // its word without re-hashing. So the data lands first: sync it,
            // write the claim, sync the claim. One barrier around both would
            // not order them, and a bitfield outliving its data means serving
            // garbage after a restart.
            file.sync_data()?;
            file.write_all_at(&bitfield.0, offset)?;
            file.sync_data()
        })
        .await
        .map_err(std::io::Error::other)?
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
