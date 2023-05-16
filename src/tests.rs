use super::*;
use tempfile::tempdir;

#[test]
fn test_stack_db() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = RocksSQ::new(dir.path().to_str().unwrap())?;

    db.push_to_stack("stack", b"item1")?;
    db.push_to_stack("stack", b"item2")?;
    assert_eq!(db.pop_stack("stack")?, Some(b"item2".to_vec()));
    assert_eq!(db.pop_stack("stack")?, Some(b"item1".to_vec()));
    assert_eq!(db.pop_stack("stack")?, None);

    Ok(())
}

#[test]
fn test_queue_db() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = RocksSQ::new(dir.path().to_str().unwrap())?;

    db.push_to_queue("queue", b"item1")?;
    db.push_to_queue("queue", b"item2")?;
    assert_eq!(db.pop_queue("queue")?, Some(b"item1".to_vec()));
    assert_eq!(db.pop_queue("queue")?, Some(b"item2".to_vec()));
    assert_eq!(db.pop_queue("queue")?, None);

    Ok(())
}
