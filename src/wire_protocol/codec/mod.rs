mod decoder;
mod encoder;

#[allow(unused_imports)]
pub use decoder::*;
#[allow(unused_imports)]
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

impl Default for WireCodec {
    fn default() -> Self {
        Self::new()
    }
}
