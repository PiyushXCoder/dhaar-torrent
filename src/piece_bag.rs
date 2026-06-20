use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::{Mutex, RwLock};

pub struct Piece {
    pub index: u32,
    pub begin: u64,
    pub length: u64,
}

pub struct PieceBag {
    pub temp_file_path: Arc<RwLock<String>>,
    pub downloaded: AtomicU64,
    pub uploaded: AtomicU64,
    pub pieces: Vec<Arc<Mutex<Piece>>>,
}

impl PieceBag {
    pub fn new(temp_file_path: String) -> Self {
        Self {
            temp_file_path: Arc::new(RwLock::new(temp_file_path)),
            downloaded: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            pieces: Vec::new(),
        }
    }
}
