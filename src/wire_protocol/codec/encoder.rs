use super::super::WireItem;
use super::WireCodec;
use tokio_util::codec::Encoder;

impl Encoder<WireItem> for WireCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: WireItem,
        dst: &mut tokio_util::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}
