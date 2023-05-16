use byteorder::{BigEndian, ByteOrder};
use rocksdb::{Error, DB};

#[cfg(test)]
mod tests;

pub struct RocksSQ {
    db: DB,
}

impl RocksSQ {
    pub fn new(path: &str) -> Result<RocksSQ, Error> {
        let db = DB::open_default(path)?;
        Ok(RocksSQ { db })
    }

    // Wrap RocksDB's native methods
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.db.get(key)
    }

    // Implement stack-like functionality
    pub fn push_to_stack(&self, stack_name: &str, value: &[u8]) -> Result<(), Error> {
        let counter_key = format!("{}_counter", stack_name);
        let counter = match self.db.get(counter_key.clone())? {
            Some(counter_bytes) => BigEndian::read_u64(&counter_bytes) + 1,
            None => 1,
        };
        let key = format!("{}_{}", stack_name, counter);
        self.db.put(key, value)?;
        let mut counter_bytes = [0u8; 8];
        BigEndian::write_u64(&mut counter_bytes, counter);
        self.db.put(counter_key, &counter_bytes)
    }

    pub fn pop_stack(&self, stack_name: &str) -> Result<Option<Vec<u8>>, Error> {
        let counter_key = format!("{}_counter", stack_name);
        match self.db.get(counter_key.clone())? {
            Some(counter_bytes) => {
                let counter = BigEndian::read_u64(&counter_bytes);
                if counter > 0 {
                    let key = format!("{}_{}", stack_name, counter);
                    let value = self.db.get(key.clone())?;
                    self.db.delete(key)?;
                    let mut counter_bytes = [0u8; 8];
                    BigEndian::write_u64(&mut counter_bytes, counter - 1);
                    self.db.put(counter_key, &counter_bytes)?;
                    Ok(value)
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    // implement queue-like functionality
    pub fn push_to_queue(&self, queue_name: &str, value: &[u8]) -> Result<(), Error> {
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

    pub fn pop_queue(&self, queue_name: &str) -> Result<Option<Vec<u8>>, Error> {
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

