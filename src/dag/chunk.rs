use super::meta_generated;
use flatbuffers;
use flatbuffers::FlatBufferBuilder;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Chunk {
    hash: String,
    data: Vec<u8>,
    meta: Option<(Vec<u8>, usize)>,
}

#[allow(dead_code)]
impl Chunk {
    pub fn new(hash: String, data: Vec<u8>, refs: &[&str]) -> Chunk {
        Chunk {
            hash,
            data,
            meta: Chunk::create_meta(refs),
        }
    }

    pub fn read(hash: String, data: Vec<u8>, meta: Option<Vec<u8>>) -> Chunk {
        Chunk {
            hash,
            data,
            meta: meta.map(|v| (v, 0)),
        }
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn refs<'a>(&'a self) -> Box<dyn Iterator<Item = &str> + 'a> {
        match self.meta() {
            None => Box::new(std::iter::empty()),
            Some(buf) => match meta_generated::meta::get_root_as_meta(buf).refs() {
                None => Box::new(std::iter::empty()),
                Some(v) => Box::new(v.iter()),
            },
        }
    }

    pub fn meta(&self) -> Option<&[u8]> {
        match &self.meta {
            None => None,
            Some((buf, offset)) => Some(&buf[*offset..]),
        }
    }

    fn create_meta(refs: &[&str]) -> Option<(Vec<u8>, usize)> {
        if refs.is_empty() {
            return None;
        }
        let mut builder = FlatBufferBuilder::default();
        let refs = builder.create_vector_of_strings(refs);
        let meta = meta_generated::meta::Meta::create(
            &mut builder,
            &meta_generated::meta::MetaArgs { refs: Some(refs) },
        );
        builder.finish(meta, None);
        Some(builder.collapse())
    }
}

impl PartialEq for Chunk {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.data == other.data && self.refs().eq(other.refs())
    }
}

impl Eq for Chunk {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        fn test(hash: String, data: Vec<u8>, refs: &[&str]) {
            let c = Chunk::new(hash.clone(), data.clone(), refs.clone());
            assert_eq!(&hash, c.hash());
            assert_eq!(data, c.data());
            assert_eq!(refs, c.refs().collect::<Vec<&str>>().as_slice());

            let buf = c.meta();
            let c2 = Chunk::read(hash.clone(), data.clone(), buf.map(|b| b.to_vec()));
            assert_eq!(c, c2);
        }

        test("".to_string(), vec![], &vec![]);
        test("h".to_string(), vec![0], &vec!["r1"]);
        test("h1".to_string(), vec![0, 1], &vec!["r1", "r2"]);
    }
}
