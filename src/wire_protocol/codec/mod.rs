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

    /// A peer controls every byte of the handshake, including the protocol
    /// string, and nothing obliges it to be UTF-8. Rejecting it is the same
    /// answer as a pstr that decodes but names another protocol; panicking on
    /// it would let any stranger kill the connection task at will.
    #[test]
    fn a_handshake_with_a_non_utf8_pstr_is_rejected_not_fatal() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&[19]);
        // Lone continuation bytes: never valid UTF-8, whatever precedes them.
        buffer.extend_from_slice(&[0x80u8; 19]);
        buffer.extend_from_slice(&[0u8; 8]);
        buffer.extend_from_slice(&[1u8; 20]);
        buffer.extend_from_slice(&[2u8; 20]);

        let error = WireCodec::new()
            .decode(&mut buffer)
            .expect_err("a pstr that is not UTF-8 should be refused");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    /// The control: a pstr that decodes cleanly but names another protocol is
    /// refused the same way, which is why both share an error.
    #[test]
    fn a_handshake_for_another_protocol_is_rejected() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&[19]);
        buffer.extend_from_slice(b"NotBitTorrent proto");
        buffer.extend_from_slice(&[0u8; 8]);
        buffer.extend_from_slice(&[1u8; 20]);
        buffer.extend_from_slice(&[2u8; 20]);

        let error = WireCodec::new()
            .decode(&mut buffer)
            .expect_err("a foreign protocol should be refused");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

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
