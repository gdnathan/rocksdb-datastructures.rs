use byteorder::{BigEndian, ByteOrder};
use rocksdb::{Error, DB};

use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Stack {
    db: Arc<Mutex<DB>>,
    name: String,
}

impl Stack {
    pub fn new(db: Arc<Mutex<DB>>, name: &str) -> Self {
        Stack {
            db,
            name: name.to_string(),
        }
    }

    pub async fn push(&self, value: &[u8]) -> Result<(), Error> {
        let db = self.db.lock().await;
        let counter_key = format!("{}_counter", self.name);
        let counter = match db.get(counter_key.clone())? {
            Some(counter_bytes) => BigEndian::read_u64(&counter_bytes) + 1,
            None => 1,
        };
        let key = format!("{}_{}", self.name, counter);
        db.put(key, value)?;
        let mut counter_bytes = [0u8; 8];
        BigEndian::write_u64(&mut counter_bytes, counter);
        db.put(counter_key, &counter_bytes)
    }

    pub async fn pop(&self) -> Result<Option<Vec<u8>>, Error> {
        let db = self.db.lock().await;
        let counter_key = format!("{}_counter", self.name);
        match db.get(counter_key.clone())? {
            Some(counter_bytes) => {
                let counter = BigEndian::read_u64(&counter_bytes);
                if counter > 0 {
                    let key = format!("{}_{}", self.name, counter);
                    let value = db.get(key.clone())?;
                    db.delete(key)?;
                    let mut counter_bytes = [0u8; 8];
                    BigEndian::write_u64(&mut counter_bytes, counter - 1);
                    db.put(counter_key, &counter_bytes)?;
                    Ok(value)
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}
