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
    assert_eq!(from_bytes::<Vec<u8>>(b"4:spam").unwrap(), b"spam");
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
