# RocksDB_sq (Stack & Queue)

A Rust crate that adds stack and queue functionality to RocksDB.

This crate provide a wrapper around a RocksDB database and provide methods for pushing/popping items to a queue or stack.

This is done by using a counter as keys into a db (for example, pushing to a "test" stack will put an item under "test_0" key, and the next item will be under "test_1" key). Counters are also store into the db.

## Usage

First, add the following to your `Cargo.toml`:

```toml
[dependencies]
rocksdb_sq = "0.1.0"
```

Then, in your code:

```rust
use rocksdb_sq::{StackDB, QueueDB};
```

### StackDB

```rust
let db = RocksSQ::new("/path/to/db")?;
db.push_to_stack("my_stack", b"item1")?;
db.push_to_stack("my_stack", b"item2")?;
let item = db.pop_stack("my_stack")?;
```

### QueueDB

```rust
let db = RocksSQ::new("/path/to/db")?;
db.push_to_queue("my_queue", b"item1")?;
db.push_to_queue("my_queue", b"item2")?;
let item = db.pop_queue("my_queue")?;
```

## Testing

You can run the tests with:

```bash
cargo test
```
