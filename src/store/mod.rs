use std::{os::unix::fs::FileExt, path::PathBuf, sync::Arc};

use sha1::Digest;
use tokio::{fs::File, io::AsyncReadExt, task::spawn_blocking};

use crate::{torrent_parser::metadata::File as TorrentFile, wire_protocol::Bitfield};

#[async_trait::async_trait]
pub trait Store {
    type Error;

    /// `piece_hashes` re-verifies what the stored bitfield claims after an
    /// unclean exit. Its length is the piece count, so a store that disagrees
    /// about the geometry is rejected rather than read at the wrong stride.
    async fn initialize(
        &self,
        piece_hashes: Vec<[u8; 20]>,
    ) -> Result<Option<Bitfield>, Self::Error>;
    async fn read(
        &self,
        piece_index: u32,
        piece_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error>;
    async fn write(
        &self,
        piece_index: u32,
        piece_offset: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error>;
    /// Records one piece as held, in the store's own bitfield.
    ///
    /// One bit, not the whole region: this runs once per completed piece, and
    /// rewriting all of it each time would be quadratic in the piece count.
    async fn record_piece(&self, piece_index: u32) -> Result<(), Self::Error>;
    async fn finalize(&self) -> Result<(), Self::Error>;
}

/// Where each region of the store begins, and how big it is.
///
/// The file is `[payload][bitfield][info_hash][flags]`; every offset follows
/// from that plus the torrent's geometry. Kept here rather than at each use so
/// a region cannot be addressed one way in one place and another elsewhere.
#[derive(Clone, Copy)]
struct StoreLayout {
    payload_length: u64,
    piece_length: u64,
}

impl StoreLayout {
    /// The trailer's two fixed-size fields. `INFO_HASH` is the width of a
    /// SHA-1 digest; `FLAGS` is the single [`StoreFlags`] byte.
    const INFO_HASH_LENGTH: u64 = 20;
    const FLAGS_LENGTH: u64 = 1;

    /// Pieces the store holds. The last one is short unless the payload
    /// divides evenly, but it still occupies a whole slot.
    fn piece_count(self) -> u64 {
        self.payload_length.div_ceil(self.piece_length)
    }

    /// Bytes of bitfield, one bit per piece. Derived rather than supplied, so
    /// it cannot disagree with the geometry the offsets are computed from.
    fn bitfield_length(self) -> u64 {
        self.piece_count().div_ceil(8)
    }

    /// Byte `piece_offset` of `piece_index`, as an offset into the store. The
    /// payload starts at zero, so this is also the offset into the payload.
    fn piece_at(self, piece_index: u32, piece_offset: u64) -> u64 {
        self.piece_length * piece_index as u64 + piece_offset
    }

    /// Bytes in `piece_index`. Only the last piece is ever short.
    fn piece_size(self, piece_index: u32) -> u64 {
        self.piece_length.min(
            self.payload_length
                .saturating_sub(self.piece_at(piece_index, 0)),
        )
    }

    fn bitfield_at(self) -> u64 {
        self.payload_length
    }

    fn info_hash_at(self) -> u64 {
        self.bitfield_at() + self.bitfield_length()
    }

    fn flags_at(self) -> u64 {
        self.info_hash_at() + Self::INFO_HASH_LENGTH
    }

    fn file_length(self) -> u64 {
        self.flags_at() + Self::FLAGS_LENGTH
    }
}

/// The store's trailing flag byte.
///
/// One bit is defined so far. A newer build may set others, so every write
/// reads first and preserves what it did not set — replacing the byte
/// wholesale is invisible today and silent data loss once a second flag
/// exists.
#[derive(Clone, Copy, Default)]
struct StoreFlags(u8);

impl StoreFlags {
    /// Set by `finalize` on a tidy exit, cleared whenever the store is opened.
    /// This sense round, so a byte that was zeroed, truncated or never written
    /// reads as *not* clean — damage costs a re-verification rather than a
    /// bitfield taken on trust.
    const CLEAN: u8 = 0b0000_0001;

    fn from_byte(byte: u8) -> Self {
        Self(byte)
    }

    fn to_byte(self) -> u8 {
        self.0
    }

    fn is_clean(self) -> bool {
        self.0 & Self::CLEAN != 0
    }

    /// Returns these flags with the clean bit set or cleared, every other bit
    /// carried through untouched.
    fn with_clean(self, clean: bool) -> Self {
        if clean {
            Self(self.0 | Self::CLEAN)
        } else {
            Self(self.0 & !Self::CLEAN)
        }
    }
}

/// The store is one file laid out as `[payload][bitfield][info_hash][flags]`,
/// split into the torrent's real shape only by `finalize`. `flags` is a single
/// byte whose clean bit is set only on a tidy exit, so a store found without
/// it was left by a crash and its bitfield cannot be trusted.
pub struct DiskStore {
    pub temp_file: PathBuf,
    /// Opened once by `initialize` and held for the download's life. Access is
    /// positional (`pread`/`pwrite`), so there is no shared cursor to fight
    /// over — which is what lets every
    /// method here take `&self`, and the writer itself be shared by the piece
    /// manager and every connection at once.
    ///
    /// Behind a mutex only because `initialize` sets it; it is taken for the
    /// length of a clone and never held across an await, so the contention is
    /// a few nanoseconds against a syscall.
    file: std::sync::Mutex<Option<Arc<std::fs::File>>>,
    /// Serialises the read-modify-write in `record_piece`. Every connection
    /// claims its own pieces, and eight of them share a byte.
    claims: Arc<std::sync::Mutex<()>>,
    /// The torrent's own bytes, which is only the first region of the store —
    /// the file on disk is this plus the bitfield, the info hash and the flag
    /// byte. It doubles as the offset the bitfield starts at.
    pub payload_length: u64,
    /// Fixed for the life of the store: it is the stride every offset is
    /// computed from, so it belongs here rather than on each call. A store
    /// read at one stride and written at another is not recoverable, and a
    /// per-call parameter is what made that expressible.
    pub piece_length: u64,
    pub name: String,
    pub md5sum: Option<String>,
    pub files: Option<Vec<TorrentFile>>,
    pub info_hash: [u8; 20],
}

impl DiskStore {
    pub fn new(
        payload_length: u64,
        piece_length: u64,
        name: &String,
        md5sum: &Option<String>,
        files: &Option<Vec<TorrentFile>>,
        info_hash: [u8; 20],
    ) -> Self {
        assert!(piece_length > 0, "piece length must be non-zero");
        let temp_file = std::env::current_dir()
            .unwrap()
            .join(format!("{name}.dhaar"));
        Self {
            temp_file,
            file: std::sync::Mutex::new(None),
            claims: Arc::new(std::sync::Mutex::new(())),
            payload_length,
            piece_length,
            name: name.clone(),
            md5sum: md5sum.clone(),
            files: files.clone(),
            info_hash,
        }
    }

    /// The store's shape. Cheap and `Copy`, so it is built where it is needed
    /// rather than cached — there is nothing to keep in sync that way.
    fn layout(&self) -> StoreLayout {
        StoreLayout {
            payload_length: self.payload_length,
            piece_length: self.piece_length,
        }
    }

    fn handle(&self) -> std::io::Result<Arc<std::fs::File>> {
        self.file
            .lock()
            .expect("store handle poisoned")
            .clone()
            .ok_or_else(|| std::io::Error::other("store used before initialize"))
    }
}

#[async_trait::async_trait]
impl Store for DiskStore {
    type Error = std::io::Error;

    async fn initialize(
        &self,
        piece_hashes: Vec<[u8; 20]>,
    ) -> Result<Option<Bitfield>, Self::Error> {
        let layout = self.layout();
        let piece_count = layout.piece_count();
        if piece_hashes.len() as u64 != piece_count {
            return Err(std::io::Error::other(format!(
                "torrent has {} hashes but this store holds {piece_count} pieces",
                piece_hashes.len(),
            )));
        }

        let path = self.temp_file.clone();
        let info_hash = self.info_hash;
        let file_length = layout.file_length();

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
                file.write_all_at(
                    &vec![0u8; layout.bitfield_length() as usize],
                    layout.bitfield_at(),
                )?;
                file.write_all_at(&info_hash, layout.info_hash_at())?;
                // Opened for writing, so not clean. Nothing to preserve here:
                // the byte is ours, laid down for the first time.
                file.write_all_at(&[StoreFlags::default().to_byte()], layout.flags_at())?;
                file.sync_all()?;
                return Ok::<_, std::io::Error>((file, None));
            }

            // Right size, but it has to be the same torrent before its
            // bitfield means anything.
            let mut stored = [0u8; 20];
            file.read_exact_at(&mut stored, layout.info_hash_at())?;
            if stored != info_hash {
                return Ok((file, None));
            }

            let mut bits = vec![0u8; layout.bitfield_length() as usize];
            file.read_exact_at(&mut bits, layout.bitfield_at())?;

            let mut bitfield = Bitfield(bits);

            let mut flags = [0];
            file.read_exact_at(&mut flags, layout.flags_at())?;
            let flags = StoreFlags::from_byte(flags[0]);
            if flags.is_clean() {
                // The last exit was tidy, so the bitfield is believable as it
                // stands. Claim the store by clearing the clean bit; any other
                // bit in there came from elsewhere and is left alone.
                file.write_all_at(&[flags.with_clean(false).to_byte()], layout.flags_at())?;
                return Ok((file, Some(bitfield)));
            }

            // Not clean: the last process died holding this store, so the
            // bitfield may claim pieces whose bytes never landed. The flag
            // byte already reads as open, so there is nothing to write back.
            //
            // The hash count matches the piece count, checked above, so every
            // index below is one the layout knows about.
            for (index, hash) in piece_hashes.iter().enumerate() {
                let index = index as u32;
                if bitfield.has_piece(index) {
                    let mut piece = vec![0u8; layout.piece_size(index) as usize];
                    file.read_exact_at(&mut piece, layout.piece_at(index, 0))?;
                    let stored_hash: [u8; 20] = sha1::Sha1::digest(&piece).into();
                    if stored_hash != *hash {
                        bitfield.set_piece(index, false);
                    }
                }
            }

            file.write_all_at(&bitfield.0, layout.bitfield_at())?;

            Ok((file, Some(bitfield)))
        })
        .await
        .map_err(std::io::Error::other)??;

        *self.file.lock().expect("store handle poisoned") = Some(Arc::new(file));
        Ok(bitfield)
    }

    async fn read(
        &self,
        piece_index: u32,
        piece_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error> {
        let file = self.handle()?;
        let offset = self.layout().piece_at(piece_index, piece_offset);
        spawn_blocking(move || {
            let mut buf = vec![0; length as usize];
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn write(
        &self,
        piece_index: u32,
        piece_offset: u64,
        data: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let file = self.handle()?;
        let offset = self.layout().piece_at(piece_index, piece_offset);
        // Deliberately unsynced. Nothing claims a block until the bitfield
        // does, and `set_bitfield` is where that claim is made durable — one
        // barrier per completed piece instead of one per block. A block lost
        // to a crash before then costs a re-download and nothing else.
        spawn_blocking(move || file.write_all_at(&data, offset))
            .await
            .map_err(std::io::Error::other)?
    }

    async fn record_piece(&self, piece_index: u32) -> Result<(), Self::Error> {
        let layout = self.layout();
        if piece_index as u64 >= layout.piece_count() {
            return Err(std::io::Error::other(format!(
                "piece {piece_index} is outside a store of {} pieces",
                layout.piece_count()
            )));
        }
        let file = self.handle()?;
        let offset = layout.bitfield_at() + (piece_index / 8) as u64;
        let bit = 1u8 << (7 - (piece_index % 8));
        let claims = self.claims.clone();
        spawn_blocking(move || {
            // Read-modify-write of one byte, and eight pieces share a byte, so
            // two connections finishing neighbours at the same time would lose
            // one of the two bits. The lock is held for two syscalls on a
            // single byte and never across an await.
            let _guard = claims.lock().expect("store claims poisoned");
            let mut byte = [0u8];
            file.read_exact_at(&mut byte, offset)?;
            byte[0] |= bit;
            // Deliberately unsynced. A barrier here would order the payload
            // before the claim, but it costs a device flush per piece -- the
            // dominant term in download throughput. Instead the claim is only
            // a hint: `initialize` re-hashes what the bitfield claims whenever
            // the flag byte says the last exit was unclean, so a claim that
            // outlives its data is corrected on the way back in rather than
            // prevented on the way out.
            file.write_all_at(&byte, offset)?;
            Ok(())
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn finalize(&self) -> Result<(), Self::Error> {
        let base_dir = self
            .temp_file
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut src = File::open(&self.temp_file).await?;

        let file = self.handle()?;
        let flags_at = self.layout().flags_at();
        // Marking the store clean. Deliberately unsynced: if this byte is lost
        // to a crash the store reads as still open, which costs one needless
        // re-verification on the way back in and nothing else. The polarity is
        // what makes that safe — a lost write cannot fake a tidy exit.
        spawn_blocking(move || {
            let mut flags = [0];
            file.read_exact_at(&mut flags, flags_at)?;
            let flags = StoreFlags::from_byte(flags[0]).with_clean(true);
            file.write_all_at(&[flags.to_byte()], flags_at)
        })
        .await
        .map_err(std::io::Error::other)??;

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
                tokio::io::copy(&mut src.take(self.payload_length), &mut out).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store laid out in a directory of its own, since `DiskStore`
    /// names its file relative to the working directory.
    async fn writer(dir: &std::path::Path, pieces: u64, piece_length: u64) -> DiskStore {
        let w = DiskStore::new(
            pieces * piece_length,
            piece_length,
            &dir.join("store").to_string_lossy().into_owned(),
            &None,
            &None,
            [7u8; 20],
        );
        w.initialize(vec![[0u8; 20]; pieces as usize])
            .await
            .unwrap();
        w
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("dhaar-pw-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Eight pieces share a byte, so two connections finishing neighbours
    /// must not lose each other's bit.
    #[tokio::test]
    async fn record_piece_keeps_the_bits_its_neighbours_set() {
        let dir = temp_dir("merge");
        let w = writer(&dir, 16, BLOCK_FOR_TEST).await;

        // 0 and 6 live in the first byte, 9 in the second.
        w.record_piece(0).await.unwrap();
        w.record_piece(6).await.unwrap();
        w.record_piece(9).await.unwrap();

        let stored = Bitfield(read_bitfield(&dir, 16 * BLOCK_FOR_TEST, 2));
        assert!(stored.has_piece(0), "an earlier claim in the byte was lost");
        assert!(stored.has_piece(6), "an earlier claim in the byte was lost");
        assert!(stored.has_piece(9));
        assert!(!stored.has_piece(1), "a bit nobody claimed was set");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The store checks the caller against its own geometry rather than
    /// writing over whatever follows the bitfield.
    #[tokio::test]
    async fn record_piece_rejects_an_index_past_the_end() {
        let dir = temp_dir("length");
        let w = writer(&dir, 16, BLOCK_FOR_TEST).await;

        let err = w.record_piece(16).await.unwrap_err();
        assert!(
            err.to_string().contains("outside a store"),
            "unexpected error: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn read_bitfield(dir: &std::path::Path, offset: u64, length: usize) -> Vec<u8> {
        use std::os::unix::fs::FileExt;
        let file = std::fs::File::open(dir.join("store.dhaar")).unwrap();
        let mut bits = vec![0u8; length];
        file.read_exact_at(&mut bits, offset).unwrap();
        bits
    }

    const BLOCK_FOR_TEST: u64 = 16 * 1024;
}
