# Rocksdb-datastructure

A Rust crate that adds datastructures to rocksdb

This crate provide a wrapper around a RocksDB database and provide methods for storing dynamic datastructure under a single key.

This is done by using a counter as keys into a db (for example, pushing to a "test" stack will put an item under "test_0" key, and the next item will be under "test_1" key). Counters are also store into the db.

## Usage

First, add the following to your `Cargo.toml`:

```toml
[dependencies]
Rocksdb-datastructure = "0.0.1"
```

Then, in your code:

```rust
use rocksdb_datastructure::{Stack, Queue};
```

### Stack

```rust
let db = Stack::new("/path/to/db")?;
db.push("my_stack", b"item1")?;
db.push("my_stack", b"item2")?;
assert_eq!(b"item2", db.pop("my_stack")?);
```

### Queue

```rust
let db = Queue::new("/path/to/db")?;
db.push("my_queue", b"item1")?;
db.push("my_queue", b"item2")?;
assert_eq!(b"item1", db.pop("my_stack")?);
```

## Operations
unlike simple key-value access, using datastructure is more complex and will have more db interactions.
This list provides a how many get and put each methods makes. This will likely be removed from README and put
into methods documentation.

Stack.push -  -  -  -> get: 1 | put: 2
Stack.pop  -  -  -  -> get: 3 | put: 1
Queue.push -  -  -  -> get: 1 | put: 2
Stack.pop  -  -  -  -> get: 3 | put: 1
Vector.push_front   -> unimplemented
Vector.push_back -  -> unimplemented
Vector.pop_front -  -> unimplemented
Vector.pop_back  -  -> unimplemented
Vector.front  -  -  -> unimplemented
Vector.back   -  -  -> unimplemented

## Testing

You can run the tests with:

```bash
cargo test
```
