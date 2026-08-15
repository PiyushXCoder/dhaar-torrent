mod decoder;
mod encoder;

pub use decoder::*;
pub use encoder::*;

pub struct WireCodec {
    state: CodecState,
}

pub(super) enum CodecState {
    HandshakePending {
        pstrlen: Option<u8>,
        info_hash_sent: bool,
    },
    Normal,
}

impl WireCodec {
    pub fn new() -> Self {
        Self {
            state: CodecState::HandshakePending {
                pstrlen: None,
                info_hash_sent: false,
            },
        }
    }
}
