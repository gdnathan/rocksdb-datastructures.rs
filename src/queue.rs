use byteorder::{BigEndian, ByteOrder};
use rocksdb::{Error, DB};

pub struct Queue {
    db: DB,
}

impl Queue {
    pub fn new(path: &str) -> Result<Queue, Error> {
        let db = DB::open_default(path)?;
        Ok(Queue { db })
    }

    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)
    }
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.db.get(key)
    }

    pub fn push(&self, queue_name: &str, value: &[u8]) -> Result<(), Error> {
        let tail_key = format!("{}_tail", queue_name);
        let tail = match self.db.get(tail_key.clone())? {
            Some(tail_bytes) => BigEndian::read_u64(&tail_bytes) + 1,
            None => 1,
        };
        let key = format!("{}_{}", queue_name, tail);
        self.db.put(key, value)?;
        let mut tail_bytes = [0u8; 8];
        BigEndian::write_u64(&mut tail_bytes, tail);
        self.db.put(tail_key, &tail_bytes)
    }

    pub fn pop(&self, queue_name: &str) -> Result<Option<Vec<u8>>, Error> {
        let head_key = format!("{}_head", queue_name);
        let tail_key = format!("{}_tail", queue_name);
        let head = match self.db.get(head_key.clone())? {
            Some(head_bytes) => BigEndian::read_u64(&head_bytes),
            None => 0,
        };
        let tail = match self.db.get(tail_key.clone())? {
            Some(tail_bytes) => BigEndian::read_u64(&tail_bytes),
            None => 0,
        };
        if head < tail {
            let key = format!("{}_{}", queue_name, head + 1);
            let value = self.db.get(key.clone())?;
            self.db.delete(key)?;
            let mut head_bytes = [0u8; 8];
            BigEndian::write_u64(&mut head_bytes, head + 1);
            self.db.put(head_key, &head_bytes)?;
            Ok(value)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
use tempfile::tempdir;

#[test]
fn test_basic_behavior() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = Queue::new(dir.path().to_str().unwrap())?;

    db.push("queue", b"item1")?;
    db.push("queue", b"item2")?;
    assert_eq!(db.pop("queue")?, Some(b"item1".to_vec()));
    assert_eq!(db.pop("queue")?, Some(b"item2".to_vec()));
    assert_eq!(db.pop("queue")?, None);

    Ok(())
}
