use serde::{Serialize, ser};

pub struct BencodeSerializer {
    output: Vec<u8>,
}

impl BencodeSerializer {
    pub fn new() -> BencodeSerializer {
        BencodeSerializer { output: Vec::new() }
    }
}

pub fn to_bytes<T>(value: &T) -> Vec<u8>
where
    T: Serialize,
{
    let mut serializer = BencodeSerializer::new();
    // value.serialize(&mut serializer).unwrap();
    serializer.output
}
