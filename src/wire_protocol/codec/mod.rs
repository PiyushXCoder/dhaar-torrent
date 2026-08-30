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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_protocol::{Message, WireItem};
    use tokio_util::bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    /// Keep-alive is the one message with no id byte, so it is the one whose
    /// frame can be got wrong without any of the others noticing: a length of
    /// one and an id of zero is a `Choke`, and it would be read as one.
    #[test]
    fn keep_alive_is_a_bare_length_prefix() {
        let mut buffer = BytesMut::new();
        WireCodec::new()
            .encode(WireItem::Message(Message::KeepAlive), &mut buffer)
            .unwrap();

        assert_eq!(&buffer[..], &[0, 0, 0, 0]);
    }

    #[test]
    fn keep_alive_survives_a_round_trip() {
        let mut buffer = BytesMut::new();
        let mut codec = WireCodec {
            state: CodecState::Normal,
        };
        codec
            .encode(WireItem::Message(Message::KeepAlive), &mut buffer)
            .unwrap();

        let decoded = codec.decode(&mut buffer).unwrap();

        assert!(matches!(
            decoded,
            Some(WireItem::Message(Message::KeepAlive))
        ));
        assert!(buffer.is_empty(), "the frame should be fully consumed");
    }

    /// A keep-alive arriving ahead of real traffic must not swallow it.
    #[test]
    fn a_keep_alive_does_not_hide_the_message_behind_it() {
        let mut buffer = BytesMut::new();
        let mut codec = WireCodec {
            state: CodecState::Normal,
        };
        codec
            .encode(WireItem::Message(Message::KeepAlive), &mut buffer)
            .unwrap();
        codec
            .encode(WireItem::Message(Message::Unchoke), &mut buffer)
            .unwrap();

        assert!(matches!(
            codec.decode(&mut buffer).unwrap(),
            Some(WireItem::Message(Message::KeepAlive))
        ));
        assert!(matches!(
            codec.decode(&mut buffer).unwrap(),
            Some(WireItem::Message(Message::Unchoke))
        ));
    }
}
