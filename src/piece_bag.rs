pub struct Piece {
    pub index: u32,
    pub begin: u64,
    pub length: u64,
}
pub struct PieceBag {
    pub temp_file_path: String,
    pub uploaded: u64,
    pub downloaded: u64,
    pub pieces: Vec<Piece>,
}

impl PieceBag {
    pub fn new(temp_file_path: String) -> Self {
        Self {
            temp_file_path,
            uploaded: 0,
            downloaded: 0,
            pieces: Vec::new(),
        }
    }
}
