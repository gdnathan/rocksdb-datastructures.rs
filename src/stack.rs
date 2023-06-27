use byteorder::{BigEndian, ByteOrder};
use rocksdb::{Error, DB};

pub struct Stack {
    db: DB,
}

impl Stack {
    pub fn new(path: &str) -> Result<Stack, Error> {
        let db = DB::open_default(path)?;
        Ok(Stack { db })
    }

    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)
    }
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.db.get(key)
    }

    pub fn push(&self, stack_name: &str, value: &[u8]) -> Result<(), Error> {
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

    pub fn pop(&self, stack_name: &str) -> Result<Option<Vec<u8>>, Error> {
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

}

#[cfg(test)]
use tempfile::tempdir;

#[test]
fn test_basic_behavior() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = Stack::new(dir.path().to_str().unwrap())?;

    db.push("stack", b"item1")?;
    db.push("stack", b"item2")?;
    assert_eq!(db.pop("stack")?, Some(b"item2".to_vec()));
    assert_eq!(db.pop("stack")?, Some(b"item1".to_vec()));
    assert_eq!(db.pop("stack")?, None);

    Ok(())
}
