mod stack;
mod queue;

use std::sync::Arc;
use tokio::sync::Mutex;
use rocksdb::{DB as RocksDB, Error};

#[feature("async")]
use self::{
    stack::Stack,
    queue::Queue,
};

#[feature("async")]
pub struct DB {
    db: Arc<Mutex<RocksDB>>,
}

#[feature("async")]
impl DB {
    pub fn new(path: &str) -> Result<DB, Error> {
        let db = RocksDB::open_default(path)?;
        let db = Arc::new(Mutex::new(db));

        Ok(DB {
            db,
        })
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db.lock().await.put(key, value)
    }
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.db.lock().await.get(key)
    }

    pub fn get_queue(&mut self, name: &str) -> Queue {
        Queue {
            db: self.db.clone(),
            name: name.to_string()
        }
    }

    pub fn get_stack(&mut self, name: &str) -> Stack {
        Stack {
            db: self.db.clone(),
            name: name.to_string()
        }
    }
}

