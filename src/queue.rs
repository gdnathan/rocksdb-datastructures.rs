use byteorder::{BigEndian, ByteOrder};
use rocksdb::{Error, DB};

use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Queue {
    db: Arc<Mutex<DB>>,
    name: String,
}

impl Queue {
    pub fn new(db: Arc<Mutex<DB>>, name: &str) -> Self {
        Queue {
            db,
            name: name.to_string(),
        }
    }

    pub async fn push(&self, value: &[u8]) -> Result<(), Error> {
        let db = self.db.lock().await;
        let tail_key = format!("{}_tail", self.name);
        let tail = match db.get(tail_key.clone())? {
            Some(tail_bytes) => BigEndian::read_u64(&tail_bytes) + 1,
            None => 1,
        };
        let key = format!("{}_{}", self.name, tail);
        db.put(key, value)?;
        let mut tail_bytes = [0u8; 8];
        BigEndian::write_u64(&mut tail_bytes, tail);
        db.put(tail_key, &tail_bytes)
    }

    pub async fn pop(&self) -> Result<Option<Vec<u8>>, Error> {
        let db = self.db.lock().await;
        let head_key = format!("{}_head", self.name);
        let tail_key = format!("{}_tail", self.name);
        let head = match db.get(head_key.clone())? {
            Some(head_bytes) => BigEndian::read_u64(&head_bytes),
            None => 0,
        };
        let tail = match db.get(tail_key.clone())? {
            Some(tail_bytes) => BigEndian::read_u64(&tail_bytes),
            None => 0,
        };
        if head < tail {
            let key = format!("{}_{}", self.name, head + 1);
            let value = db.get(key.clone())?;
            db.delete(key)?;
            let mut head_bytes = [0u8; 8];
            BigEndian::write_u64(&mut head_bytes, head + 1);
            db.put(head_key, &head_bytes)?;
            Ok(value)
        } else {
            Ok(None)
        }
    }
}
