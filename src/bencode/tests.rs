use std::collections::HashMap;

use super::*;

#[test]
fn test_parse_integer() {
    assert_eq!(from_bytes::<u32>(b"i345e").unwrap(), 345);
}

#[test]
fn test_parse_string() {
    assert_eq!(from_bytes::<String>(b"4:spam").unwrap(), "spam");
}

#[test]
fn test_parse_bytes() {
    assert_eq!(
        from_bytes::<serde_bytes::ByteBuf>(b"4:spam").unwrap(),
        b"spam"
    );
}

#[test]
fn test_parse_list() {
    assert_eq!(from_bytes::<Vec<u32>>(b"li345ei4ee").unwrap(), vec![345, 4]);
}

#[test]
fn test_parse_tuple() {
    assert_eq!(
        from_bytes::<(i32, &str)>(b"li345e5:helloe").unwrap(),
        (345, "hello")
    )
}

#[test]
fn test_parse_dict() {
    assert_eq!(
        from_bytes::<HashMap<String, u32>>(b"d4:spami345ee").unwrap(),
        HashMap::from([("spam".to_string(), 345)])
    )
}

#[test]
fn test_parse_unit() {
    assert_eq!(from_bytes::<()>(b"").unwrap(), ())
}

#[test]
fn test_serialize_integer() {
    assert_eq!(to_bytes(&345u32), b"i345e");
}

#[test]
fn test_serialize_negative_integer() {
    assert_eq!(to_bytes(&-345i32), b"i-345e");
}

#[test]
fn test_serialize_string() {
    assert_eq!(to_bytes(&"spam"), b"4:spam");
}

#[test]
fn test_serialize_bytes() {
    assert_eq!(to_bytes(&serde_bytes::Bytes::new(b"spam")), b"4:spam");
}

#[test]
fn test_serialize_list() {
    assert_eq!(to_bytes(&vec![345u32, 4]), b"li345ei4ee");
}

#[test]
fn test_serialize_tuple() {
    assert_eq!(to_bytes(&(345, "hello")), b"li345e5:helloe");
}

#[test]
fn test_serialize_dict() {
    assert_eq!(
        to_bytes(&HashMap::from([("spam".to_string(), 345)])),
        b"d4:spami345ee"
    );
}

#[test]
fn test_roundtrip_dict() {
    let original = HashMap::from([("spam".to_string(), 345u32)]);
    let encoded = to_bytes(&original);
    assert_eq!(
        from_bytes::<HashMap<String, u32>>(&encoded).unwrap(),
        original
    );
}
