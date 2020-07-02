use std::fmt;
use std::str;

// KVKey is the key we use to store our dag data in the underlying
// kvstore.
#[derive(Debug, PartialEq, Eq)]
pub enum Key {
    ChunkData(String),
    ChunkRefs(String),
    Head(String),
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::ChunkData(hash) => write!(f, "c/{}/d", hash),
            Key::ChunkRefs(hash) => write!(f, "c/{}/r", hash),
            Key::Head(name) => write!(f, "h/{}", name),
        }
    }
}

type ParseError = ();

impl str::FromStr for Key {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split("/").collect::<Vec<&str>>();
        if parts.len() >= 2 {
            let name = String::from(parts[1]);
            if parts[0] == "c" {
                if parts.len() == 3 {
                    if parts[2] == "d" {
                        return Ok(Key::ChunkData(String::from(name)));
                    } else if parts[2] == "r" {
                        return Ok(Key::ChunkRefs(String::from(name)));
                    }
                }
            } else if parts[0] == "h" {
                return Ok(Key::Head(name));
            }
        }
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string() {
        fn test(k: &Key, expected: &str) {
            assert_eq!(expected, k.to_string());
        }
        test(&Key::ChunkData(String::from("")), "c//d");
        test(&Key::ChunkData(String::from("a")), "c/a/d");
        test(&Key::ChunkData(String::from("ab")), "c/ab/d");
        test(&Key::ChunkRefs(String::from("")), "c//r");
        test(&Key::ChunkRefs(String::from("a")), "c/a/r");
        test(&Key::ChunkRefs(String::from("ab")), "c/ab/r");
        test(&Key::Head(String::from("")), "h/");
        test(&Key::Head(String::from("a")), "h/a");
        test(&Key::Head(String::from("ab")), "h/ab");
    }

    #[test]
    fn from_string() {
        fn test(expected: Result<Key, ParseError>, s: &str) {
            assert_eq!(expected, s.parse::<Key>());
        }
        test(Err(()), ""); // empty string
        test(Err(()), "a"); // invalid prefix
        test(Err(()), "c"); // invalid chunk:
        test(Err(()), "c/");
        test(Err(()), "c//");
        test(Err(()), "c/a/");
        test(Err(()), "c/a/a");
        test(Ok(Key::ChunkData("".to_string())), "c//d");
        test(Ok(Key::ChunkData("a".to_string())), "c/a/d");
        test(Ok(Key::ChunkData("ab".to_string())), "c/ab/d");
        test(Ok(Key::ChunkRefs("".to_string())), "c//r");
        test(Ok(Key::ChunkRefs("a".to_string())), "c/a/r");
        test(Ok(Key::ChunkRefs("ab".to_string())), "c/ab/r");
        test(Ok(Key::Head("".to_string())), "h/");
        test(Ok(Key::Head("a".to_string())), "h/a");
        test(Ok(Key::Head("ab".to_string())), "h/ab");
    }

    #[test]
    fn roundtrip() -> Result<(), ParseError> {
        let cases: &[Key] = &[
            Key::ChunkData(String::from("".to_string())),
            Key::ChunkData(String::from("a".to_string())),
            Key::ChunkRefs(String::from("".to_string())),
            Key::ChunkRefs(String::from("a".to_string())),
            Key::Head("".to_string()),
            Key::Head("a".to_string()),
        ];

        for c in cases {
            let exp = c;
            let encoded = exp.to_string();
            assert!(encoded.len() > 0);
            let act: Key = encoded.parse()?;
            assert_eq!(exp, &act, "Could not roundtrip {}", exp);
        }

        Ok(())
    }
}