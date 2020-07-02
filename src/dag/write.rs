use super::chunk::Chunk;
use super::key::Key;
use super::read;
use super::Result;
use crate::kv;

#[allow(dead_code)]
pub struct Write<'a> {
    kvw: Box<dyn kv::Write + 'a>,
}

#[allow(dead_code)]
impl<'a> Write<'_> {
    pub fn new(kvw: Box<dyn kv::Write + 'a>) -> Write {
        Write{kvw}
    }

    pub async fn has_chunk(&mut self, hash: &str) -> Result<bool> {
        read::has_chunk(self.kvw.as_read(), hash).await
    }

    pub async fn get_chunk(&mut self, hash: &str) -> Result<Option<Chunk>> {
        read::get_chunk(self.kvw.as_read(), hash).await
    }

    pub async fn get_head(&mut self, name: &str) -> Result<Option<String>> {
        read::get_head(self.kvw.as_read(), name).await
    }

    pub async fn put_chunk(&mut self, c: Chunk) -> Result<()> {
        self.kvw
            .put(&Key::ChunkData(c.hash()).to_string(), c.data())
            .await?;
        if let Some(meta) = c.meta() {
            self.kvw
                .put(&Key::ChunkMeta(c.hash()).to_string(), meta)
                .await?;
        }
        Ok(())
    }

    pub async fn set_head(&mut self, name: &str, hash: &str) -> Result<()> {
        Ok(self.kvw
            .put(&Key::Head(name).to_string(), hash.as_bytes())
            .await?)
    }

    pub async fn commit(&mut self) -> Result<()> {
        Ok(self.kvw.commit().await?)
    }

    pub async fn rollback(&mut self) -> Result<()> {
        Ok(self.kvw.rollback().await?)
    }
}
