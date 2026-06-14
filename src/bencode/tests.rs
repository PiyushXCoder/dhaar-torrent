use std::collections::HashMap;

use super::*;

#[test]
fn test_parse_integer() {
    assert_eq!(from_bytes::<u32>(b"i345e").unwrap(), 345);
}

#[test]
fn test_parse_negative_integer() {
    assert_eq!(from_bytes::<i32>(b"i-345e").unwrap(), -345);
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
fn test_parse_any() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    #[serde(untagged)]
    enum Value {
        Integer(i64),
        String(String),
        List(Vec<Value>),
        Dict(HashMap<String, Value>),
    }

    assert_eq!(from_bytes::<Value>(b"i345e").unwrap(), Value::Integer(345));
    assert_eq!(
        from_bytes::<Value>(b"4:spam").unwrap(),
        Value::String("spam".to_string())
    );
    assert_eq!(
        from_bytes::<Value>(b"li345e4:spame").unwrap(),
        Value::List(vec![Value::Integer(345), Value::String("spam".to_string())])
    );
    assert_eq!(
        from_bytes::<Value>(b"d4:spamli345eee").unwrap(),
        Value::Dict(HashMap::from([(
            "spam".to_string(),
            Value::List(vec![Value::Integer(345)])
        )]))
    );
}

#[test]
fn test_parse_ignored_any() {
    assert_eq!(
        from_bytes::<(serde::de::IgnoredAny, u32)>(b"ld1:ai1eei7ee")
            .unwrap()
            .1,
        7
    );
}

#[test]
fn test_parse_raw() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Inner {
        x: i64,
    }

    #[derive(Debug, serde::Deserialize)]
    struct Outer {
        a: Raw<Inner>,
        b: u32,
    }

    // raw span must be exact original bytes, unknown key `y` included
    let outer = from_bytes::<Outer>(b"d1:ad1:xi5e1:yli1ei2eee1:bi7ee").unwrap();
    assert_eq!(outer.a.bytes, b"d1:xi5e1:yli1ei2eee");
    assert_eq!(outer.b, 7);
}

#[test]
fn test_parse_unit() {
    assert_eq!(from_bytes::<()>(b"").unwrap(), ())
}

#[test]
fn test_serialize_integer() {
    assert_eq!(to_bytes(&345u32).unwrap(), b"i345e");
}

#[test]
fn test_serialize_negative_integer() {
    assert_eq!(to_bytes(&-345i32).unwrap(), b"i-345e");
}

#[test]
fn test_serialize_string() {
    assert_eq!(to_bytes(&"spam").unwrap(), b"4:spam");
}

#[test]
fn test_serialize_bytes() {
    assert_eq!(
        to_bytes(&serde_bytes::Bytes::new(b"spam")).unwrap(),
        b"4:spam"
    );
}

#[test]
fn test_serialize_list() {
    assert_eq!(to_bytes(&vec![345u32, 4]).unwrap(), b"li345ei4ee");
}

#[test]
fn test_serialize_tuple() {
    assert_eq!(to_bytes(&(345, "hello")).unwrap(), b"li345e5:helloe");
}

#[test]
fn test_serialize_dict() {
    assert_eq!(
        to_bytes(&HashMap::from([("spam".to_string(), 345)])).unwrap(),
        b"d4:spami345ee"
    );
}

#[test]
fn test_roundtrip_dict() {
    let original = HashMap::from([("spam".to_string(), 345u32)]);
    let encoded = to_bytes(&original).unwrap();
    assert_eq!(
        from_bytes::<HashMap<String, u32>>(&encoded).unwrap(),
        original
    );
}
