use super::commit;
use crate::dag;
use crate::prolly;
use crate::to_native::ToNativeValue;
use async_std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen::JsValue;

#[derive(Debug)]
pub struct Index {
    pub meta: commit::IndexRecord,
    map: RwLock<Option<prolly::Map>>,
}

impl Index {
    pub fn new(meta: commit::IndexRecord, map: Option<prolly::Map>) -> Index {
        Index {
            meta,
            map: RwLock::new(map),
        }
    }
    pub async fn get_map_mut(
        &self,
        read: &dag::Read<'_>,
    ) -> Result<MapWriteGuard<'_>, GetMapError> {
        use GetMapError::*;
        let mut guard = self.map.write().await;
        if (*guard).is_none() {
            *guard = Some(
                prolly::Map::load(&self.meta.value_hash, read)
                    .await
                    .map_err(MapLoadError)?,
            );
        }
        Ok(MapWriteGuard { guard })
    }

    #[allow(dead_code)]
    pub async fn get_map(&self, read: &dag::Read<'_>) -> Result<MapReadGuard<'_>, GetMapError> {
        self.get_map_mut(read).await?;
        Ok(MapReadGuard {
            guard: self.map.read().await,
        })
    }

    // Note: does not update self.meta.value_hash (doesn't need to at this point as flush
    // is only called during commit.)
    pub async fn flush(&self, write: &mut dag::Write<'_>) -> Result<String, IndexFlushError> {
        use IndexFlushError::*;
        let mut guard = self.map.write().await;
        match &mut *guard {
            Some(m) => m.flush(write).await.map_err(MapFlushError),
            None => Ok(self.meta.value_hash.clone()),
        }
    }
}

pub struct MapReadGuard<'a> {
    pub guard: RwLockReadGuard<'a, Option<prolly::Map>>,
}

impl<'a> MapReadGuard<'a> {
    // Seems like this could just be DeRef.
    #[allow(dead_code)]
    pub fn get_map(&'a self) -> &'a prolly::Map {
        self.guard.as_ref().unwrap()
    }
}

pub struct MapWriteGuard<'a> {
    pub guard: RwLockWriteGuard<'a, Option<prolly::Map>>,
}

impl<'a> MapWriteGuard<'a> {
    // TODO: This method does not work. Call site says that guard is destroyed too early.
    #[allow(dead_code)]
    pub fn get_map(&'a mut self) -> &'a mut prolly::Map {
        self.guard.as_mut().unwrap()
    }
}

#[derive(Debug, PartialEq)]
pub enum GetMapError {
    MapLoadError(prolly::LoadError),
}

impl ToNativeValue<JsValue> for GetMapError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            GetMapError::MapLoadError(e) => e.to_native(),
        }
    }
}

#[derive(Debug)]
pub enum IndexFlushError {
    MapFlushError(prolly::FlushError),
}

impl ToNativeValue<JsValue> for IndexFlushError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            IndexFlushError::MapFlushError(e) => e.to_native(),
        }
    }
}

// IndexKey is the key used in the index prolly map for indexed values. It
// is serialized by encode_index_key such that proper sort order is preserved.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct IndexKey<'a> {
    pub secondary: &'a [u8],
    pub primary: &'a [u8],
}

#[allow(dead_code)]
#[derive(Copy, Clone)]
pub enum IndexOperation {
    Add,
    Remove,
}

#[derive(Debug, PartialEq)]
pub enum IndexValueError {
    GetIndexKeysError(GetIndexKeysError),
}

impl ToNativeValue<JsValue> for IndexValueError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            IndexValueError::GetIndexKeysError(e) => e.to_native(),
        }
    }
}

// Index or de-index a single primary entry.
pub fn index_value(
    index: &mut prolly::Map,
    op: IndexOperation,
    key: &[u8],
    val: &[u8],
    json_pointer: &str,
) -> Result<(), IndexValueError> {
    use IndexValueError::*;
    for entry in get_index_keys(key, val, json_pointer).map_err(GetIndexKeysError)? {
        match &op {
            IndexOperation::Add => index.put(entry, val.to_vec()),
            IndexOperation::Remove => index.del(entry),
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub enum GetIndexKeysError {
    DeserializeError(String),
    NoValueAtPath(String),
    SecondaryKeyContainsNull(String),
    UnsupportedTargetType,
}

impl ToNativeValue<JsValue> for GetIndexKeysError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            GetIndexKeysError::DeserializeError(_) => None,
            GetIndexKeysError::NoValueAtPath(_) => None,
            GetIndexKeysError::SecondaryKeyContainsNull(_) => None,
            GetIndexKeysError::UnsupportedTargetType => None,
        }
    }
}

// Gets the set of index keys for a given primary key and value.
fn get_index_keys(
    key: &[u8],
    val: &[u8],
    json_pointer: &str,
) -> Result<Vec<Vec<u8>>, GetIndexKeysError> {
    use GetIndexKeysError::*;
    // TODO: It's crazy to decode the entire value just to evaluate the json pointer.
    // There should be some way to shortcut this. Halp @arv.
    let value: Value = serde_json::from_slice(val).map_err(|e| DeserializeError(e.to_string()))?;
    let target = value.pointer(json_pointer);
    if target.is_none() {
        return Err(NoValueAtPath(json_pointer.to_string()));
    }
    let target = target.unwrap();

    let mut values = Vec::new();
    if target.is_array() {
        target
            .as_array()
            .unwrap()
            .iter()
            .for_each(|v| values.push(v));
    } else {
        values.push(target);
    }
    let mut strings = Vec::new();
    for value in values.drain(..) {
        match value {
            Value::String(v) => strings.push(v),
            _ => return Err(UnsupportedTargetType),
        }
    }

    strings
        .into_iter()
        .map(|v| {
            encode_index_key(&IndexKey {
                secondary: v.as_bytes(),
                primary: key,
            })
        })
        .collect()
}

static KEY_VERSION_0: &[u8] = &[0];
static KEY_SEPARATOR: &[u8] = &[0];

// An index key is encoded to vec of bytes in the following order:
//   - key version byte(s), followed by
//   - the secondary key bytes (which for now is a UTF8 encoded string), followed by
//   - the key separator, a null byte, followed by
//   - the primary key bytes
//
// The null separator byte ensures that if a secondary key A is longer than B then
// A always sorts after B. Appending the primary key ensures index keys with
// identical secondary keys sort in primary key order. Secondary keys must not
// contain a zero (null) byte.
pub fn encode_index_key(index_key: &IndexKey) -> Result<Vec<u8>, GetIndexKeysError> {
    use GetIndexKeysError::*;

    let IndexKey { secondary, primary } = *index_key;

    if secondary.contains(&0u8) {
        let msg =
            String::from_utf8(secondary.to_vec()).unwrap_or_else(|_| format!("{:?}", secondary));
        return Err(SecondaryKeyContainsNull(msg));
    }
    let mut v = Vec::new();
    v.reserve(KEY_VERSION_0.len() + secondary.len() + KEY_SEPARATOR.len() + primary.len());
    v.extend_from_slice(KEY_VERSION_0);
    v.extend_from_slice(secondary);
    v.extend_from_slice(KEY_SEPARATOR);
    v.extend_from_slice(primary);
    Ok(v)
}

// Returns bytes that can be used to scan for the given secondary index value.
//
// Consider a scan for start_secondary_key="a" (97). We want to scan with scan
// key [0, 97]. We could also scan with [0, 97, 0], but then we couldn't use
// this function for prefix scans, so we lop off the null byte. If we want
// the scan to be exclusive, we scan with the next greater value, [0, 97, 1]
// (we disallow zero bytes in secondary keys).
//
// Now it gets a little tricky. We also want to be able to scan using the
// primary key, start_key. When we do this we have to encode the scan key
// a little differently We essentially have to fix the value of the
// secondary key so we can vary the start_key. That is, the match on
// start_secondary_key becomes an exact match.
//
// Consider the scan for start_secondary_key="a" and start_key=[2]. We want
// to scan with [0, 97, 0, 2]. If we want exclusive we want to scan with
// the next highest value, [0, 97, 0, 2, 0] (zero bytes are allowed in primary
// keys). So far so good. It is important to notice that we need to
// be able to distinguish between not wanting use start_key and wanting to
// use start_key=[]. In the former case we want to scan with the secondary
// key value, possibly followed by a 1 with no trailing zero byte ([0, 97]
// or [0, 97, 1]). In the latter case we want to scan by the secondary
// key value, followed by the zero byte, followed by the primary key value
// and another zero if it is exclusive ([0, 97, 0] or [0, 97, 0, 0]).
// This explains why we need the Option around start_key.
pub fn encode_index_scan_key(
    secondary: &[u8],
    primary: Option<&[u8]>,
    exclusive: bool,
) -> Result<Vec<u8>, GetIndexKeysError> {
    let mut k = encode_index_key(&IndexKey {
        secondary,
        primary: primary.unwrap_or(&[]),
    })?;

    let mut smallest_legal_value = 0x00;
    if primary.is_none() {
        k.pop();
        smallest_legal_value = 0x01;
    }
    if exclusive {
        k.push(smallest_legal_value);
    }
    Ok(k)
}

// Decodes an IndexKey encoded by encode_index_key.
pub fn decode_index_key(encoded_index_key: &[u8]) -> Result<IndexKey, DecodeIndexKeyError> {
    use DecodeIndexKeyError::*;

    if !encoded_index_key.starts_with(KEY_VERSION_0) {
        return Err(InvalidVersion);
    }

    let version_len = KEY_VERSION_0.len();
    let separator_len = KEY_SEPARATOR.len();
    let mut separator_offset: Option<usize> = None;
    for i in version_len..encoded_index_key.len() {
        if &encoded_index_key[i..i + separator_len] == KEY_SEPARATOR {
            separator_offset = Some(i);
            break;
        }
    }
    if separator_offset.is_none() {
        return Err(InvalidFormatting(encoded_index_key.to_vec()));
    }
    let separator_offset = separator_offset.unwrap();
    let secondary = &encoded_index_key[version_len..separator_offset];
    let primary = &encoded_index_key[separator_offset + separator_len..];
    Ok(IndexKey { secondary, primary })
}

#[derive(Debug)]
pub enum DecodeIndexKeyError {
    InvalidFormatting(Vec<u8>),
    InvalidVersion,
}

impl ToNativeValue<JsValue> for DecodeIndexKeyError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            DecodeIndexKeyError::InvalidFormatting(_) => None,
            DecodeIndexKeyError::InvalidVersion => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use str_macro::str;

    #[test]
    fn test_index_key() {
        fn test_valid(secondary: &str, primary: &[u8]) {
            // Ensure the encoded value is what we expect.
            let encoded = encode_index_key(&IndexKey {
                secondary: secondary.as_bytes(),
                primary,
            })
            .unwrap();
            assert_eq!(KEY_VERSION_0, &encoded[..KEY_VERSION_0.len()]);
            let secondary_index = KEY_VERSION_0.len();
            let separator_index = secondary_index + secondary.len();
            assert_eq!(
                secondary.as_bytes(),
                &encoded[secondary_index..separator_index]
            );
            let primary_index = separator_index + KEY_SEPARATOR.len();
            assert_eq!(KEY_SEPARATOR, &encoded[separator_index..primary_index]);
            assert_eq!(primary, &encoded[primary_index..]);

            // Ensure we can decode it properly.
            let decoded = decode_index_key(&encoded).unwrap();
            assert_eq!(secondary.as_bytes(), &decoded.secondary[..]);
            assert_eq!(primary, &decoded.primary[..]);
        }
        test_valid("", &[]);
        test_valid("", &[0x00]);
        test_valid("", &[0x01]);
        test_valid("a", &[]);
        test_valid("a", &[0x61 /* 'a' */]);
        test_valid("foo", &[0x01, 0x02, 0x03]);

        fn test_invalid_encode(secondary: &str, primary: &[u8], expected: &str) {
            let err = encode_index_key(&IndexKey {
                secondary: secondary.as_bytes(),
                primary,
            })
            .unwrap_err();
            let err_str = format!("{:?}", err);
            assert!(
                err_str.contains(expected),
                "{} does not contain {}",
                err_str,
                expected
            );
        }
        test_invalid_encode("no \0 nulls", &[], "Null");

        fn test_invalid_decode(encoded: &[u8], expected: &str) {
            let err = decode_index_key(encoded).unwrap_err();
            let err_str = format!("{:?}", err);
            assert!(
                err_str.contains(expected),
                "{} does not contain {}",
                err_str,
                expected
            );
        }
        test_invalid_decode(&[], "InvalidVersion");
        test_invalid_decode(&[0x01], "InvalidVersion");
        test_invalid_decode(&[0x00], "InvalidFormatting");
        test_invalid_decode(&[0x00, 0x01, 0x02], "InvalidFormatting");
    }

    #[test]
    fn test_encode_scan_key() {
        fn test(secondary: &str, primary: &[u8]) {
            let encoded_index_key = encode_index_key(&IndexKey {
                secondary: secondary.as_bytes(),
                primary,
            })
            .unwrap();
            // With exclusive == false
            let scan_key =
                encode_index_scan_key(secondary.as_bytes(), Some(primary), false).unwrap();
            assert!(encoded_index_key.starts_with(&scan_key[..]));
            assert!(encoded_index_key >= scan_key);

            // With exclusive == true
            let scan_key =
                encode_index_scan_key(secondary.as_bytes(), Some(primary), true).unwrap();
            assert!(encoded_index_key < scan_key);
        }

        test("", &[]);
        test("", &[0x00]);
        test("", &[0x01]);
        test("foo", &[]);
        test("foo", &[0x00]);
        test("foo", &[0x01]);
    }

    #[test]
    fn test_index_key_sort() {
        fn test(left: (&str, &[u8]), right: (&str, &[u8])) {
            assert!(
                encode_index_key(&IndexKey {
                    secondary: left.0.as_bytes(),
                    primary: left.1
                })
                .unwrap()
                    < encode_index_key(&IndexKey {
                        secondary: right.0.as_bytes(),
                        primary: right.1
                    })
                    .unwrap()
            );
        }

        test(("", &[]), ("", &[0x00]));
        test(("", &[0x00]), ("a", &[]));
        test(("a", &[0x00]), ("aa", &[]));
        test(("A", &[]), ("a", &[]));
        test(("foo", &[]), ("foobar", &[]));
        test(("😀", &[]), ("😜", &[]));
        test(("a", &[0xFF]), ("aa", &[0x00]));
    }

    // By design the index key is encoded in a way that doesn't permit collisions, eg
    // a situation where scan({indexName: "...", ...prefix="foo"}) matches a value
    // with secondary index "f" and primary index "oo". This test gives us a tiny
    // extra assurance that this is the case.
    #[test]
    fn test_index_key_uniqueness() {
        fn test(left: (&str, &[u8]), right: (&str, &[u8])) {
            assert_ne!(
                encode_index_key(&IndexKey {
                    secondary: left.0.as_bytes(),
                    primary: left.1
                })
                .unwrap(),
                encode_index_key(&IndexKey {
                    secondary: right.0.as_bytes(),
                    primary: right.1
                })
                .unwrap()
            );
        }

        test(("", &[0x61]), ("a", &[]));
    }

    #[test]
    fn test_get_index_keys() {
        use GetIndexKeysError::*;
        fn test(
            key: &str,
            input: &[u8],
            json_pointer: &str,
            expected: Result<Vec<IndexKey>, GetIndexKeysError>,
        ) {
            assert_eq!(
                super::get_index_keys(key.as_bytes(), input, json_pointer),
                expected.map(|v| v
                    .iter()
                    .map(|k| encode_index_key(k).unwrap())
                    .collect::<Vec<Vec<u8>>>())
            );
        }

        // invalid json
        test(
            "k",
            &[] as &[u8],
            "/",
            Err(DeserializeError(
                "EOF while parsing a value at line 1 column 0".to_string(),
            )),
        );

        // no matching target
        test("k", b"{}", "/foo", Err(NoValueAtPath(str!("/foo"))));

        // unsupported target types
        test(
            "k",
            &serde_json::to_vec(&json!({"unsupported": {}})).unwrap(),
            "/unsupported",
            Err(UnsupportedTargetType),
        );
        test(
            "k",
            &serde_json::to_vec(&json!({ "unsupported": null })).unwrap(),
            "/unsupported",
            Err(UnsupportedTargetType),
        );
        test(
            "k",
            &serde_json::to_vec(&json!({ "unsupported": true })).unwrap(),
            "/unsupported",
            Err(UnsupportedTargetType),
        );
        test(
            "k",
            &serde_json::to_vec(&json!({ "unsupported": 42 })).unwrap(),
            "/unsupported",
            Err(UnsupportedTargetType),
        );

        test(
            "k",
            &serde_json::to_vec(&json!({ "unsupported": 88.8 })).unwrap(),
            "/unsupported",
            Err(UnsupportedTargetType),
        );
        test(
            "k",
            &serde_json::to_vec(&json!("no \0 allowed")).unwrap(),
            "",
            Err(SecondaryKeyContainsNull(str!("no \0 allowed"))),
        );

        // success
        // array of string
        test(
            "k",
            &serde_json::to_vec(&json!({"foo": []})).unwrap(),
            "/foo",
            Ok(vec![]),
        );
        test(
            "k",
            &serde_json::to_vec(&json!({"foo": ["bar", "", "baz"]})).unwrap(),
            "/foo",
            Ok(vec![
                IndexKey {
                    secondary: b"bar",
                    primary: b"k",
                },
                IndexKey {
                    secondary: b"",
                    primary: b"k",
                },
                IndexKey {
                    secondary: b"baz",
                    primary: b"k",
                },
            ]),
        );

        // string
        test(
            "foo",
            &serde_json::to_vec(&json!({"foo":"bar"})).unwrap(),
            "/foo",
            Ok(vec![IndexKey {
                secondary: b"bar",
                primary: b"foo",
            }]),
        );
        test(
            "foo",
            &serde_json::to_vec(&json!({"foo":{"bar":["hot", "dog"]}})).unwrap(),
            "/foo/bar/1",
            Ok(vec![IndexKey {
                secondary: b"dog",
                primary: b"foo",
            }]),
        );
        test(
            "",
            &serde_json::to_vec(&json!({"foo":"bar"})).unwrap(),
            "/foo",
            Ok(vec![IndexKey {
                secondary: b"bar",
                primary: b"",
            }]),
        );
        test(
            "/! ",
            &serde_json::to_vec(&json!({"foo":"bar"})).unwrap(),
            "/foo",
            Ok(vec![IndexKey {
                secondary: b"bar",
                primary: b"/! ",
            }]),
        );
    }

    #[test]
    fn index_value() {
        fn test(
            key: &str,
            value: &[u8],
            json_pointer: &str,
            op: IndexOperation,
            expected: Result<Vec<u32>, IndexValueError>,
        ) {
            let mut index = prolly::Map::new();
            index.put(
                encode_index_key(&IndexKey {
                    secondary: b"s1",
                    primary: b"1",
                })
                .unwrap(),
                b"v1".to_vec(),
            );
            index.put(
                encode_index_key(&IndexKey {
                    secondary: b"s2",
                    primary: b"2",
                })
                .unwrap(),
                b"v2".to_vec(),
            );

            let res = super::index_value(&mut index, op, key.as_bytes(), value, json_pointer);
            match expected {
                Err(expected_err) => assert_eq!(expected_err, res.unwrap_err()),
                Ok(expected_val) => {
                    let actual_val = index.iter().collect::<Vec<prolly::Entry>>();
                    assert_eq!(expected_val.len(), actual_val.len());
                    for (exp_id, act) in expected_val.iter().zip(actual_val) {
                        let exp_entry = encode_index_key(&IndexKey {
                            secondary: format!("s{}", exp_id).as_bytes(),
                            primary: format!("{}", exp_id).as_bytes(),
                        })
                        .unwrap();
                        assert_eq!(exp_entry, act.key.to_vec());
                        assert_eq!(index.get(exp_entry.as_ref()).unwrap(), act.val);
                    }
                }
            }
        }

        test(
            "3",
            json!({"s": "s3", "v": "v3"}).to_string().as_bytes(),
            "/s",
            IndexOperation::Add,
            Ok(vec![1, 2, 3]),
        );
        test(
            "1",
            json!({"s": "s1", "v": "v1"}).to_string().as_bytes(),
            "/s",
            IndexOperation::Remove,
            Ok(vec![2]),
        );
    }
}
