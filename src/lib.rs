mod queue;
mod stack;

use rocksdb::{Error, DB as RocksDB};
use std::sync::Arc;
use tokio::sync::Mutex;

pub use self::{queue::Queue, stack::Stack};

#[derive(Clone)]
pub struct DB {
    db: Arc<Mutex<RocksDB>>,
}

impl DB {
    pub fn new(path: &str) -> Result<DB, Error> {
        let db = RocksDB::open_default(path)?;
        let db = Arc::new(Mutex::new(db));

        Ok(DB { db })
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db.lock().await.put(key, value)
    }
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.db.lock().await.get(key)
    }

    pub fn get_queue(&self, name: &str) -> Queue {
        Queue::new(self.db.clone(), name)
    }

    pub fn get_stack(&self, name: &str) -> Stack {
        Stack::new(self.db.clone(), name)
    }
}

#[cfg(test)]
use tempfile::tempdir;

#[tokio::test]
async fn test_basic_stack_behavior() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = DB::new(dir.path().to_str().unwrap())?;
    let stack = db.get_stack("stack");

    stack.push(b"item1").await?;
    stack.push(b"item2").await?;
    assert_eq!(stack.pop().await?, Some(b"item2".to_vec()));
    assert_eq!(stack.pop().await?, Some(b"item1".to_vec()));
    assert_eq!(stack.pop().await?, None);

    Ok(())
}

#[tokio::test]
async fn test_basic_queue_behavior() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = DB::new(dir.path().to_str().unwrap())?;
    let queue = db.get_queue("queue");

    queue.push(b"item1").await?;
    queue.push(b"item2").await?;
    assert_eq!(queue.pop().await?, Some(b"item1".to_vec()));
    assert_eq!(queue.pop().await?, Some(b"item2".to_vec()));
    assert_eq!(queue.pop().await?, None);

    Ok(())
}

#[tokio::test]
async fn test_multiple_objects() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = DB::new(dir.path().to_str().unwrap())?;
    let queue = db.get_queue("queue");

    queue.push(b"item1").await?;
    let stack = db.get_stack("stack");
    stack.push(b"item1").await?;
    stack.push(b"item2").await?;
    assert_eq!(stack.pop().await?, Some(b"item2".to_vec()));
    queue.push(b"item2").await?;
    assert_eq!(stack.pop().await?, Some(b"item1".to_vec()));
    assert_eq!(queue.pop().await?, Some(b"item1".to_vec()));
    assert_eq!(stack.pop().await?, None);
    assert_eq!(queue.pop().await?, Some(b"item2".to_vec()));
    assert_eq!(queue.pop().await?, None);

    Ok(())
}
