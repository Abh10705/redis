// **FIX 3:** Update the use statements.
use crate::db::{Entry, InMemoryDB, RedisValue};
use std::time::Instant;

impl InMemoryDB {
    pub fn rpush(&mut self, key: String, elements: Vec<String>) -> Result<usize, &'static str> {
        // **FIX 4:** Correctly reference the `Entry` struct.
        let entry = self.map.entry(key).or_insert_with(|| Entry {
            value: RedisValue::List(Vec::new()),
            expires_at: None,
        });

        if let RedisValue::List(list) = &mut entry.value {
            list.extend(elements);
            Ok(list.len())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }

    pub fn lpush(&mut self, key: String, elements: Vec<String>) -> Result<usize, &'static str> {
        let entry = self.map.entry(key).or_insert_with(|| Entry {
            value: RedisValue::List(Vec::new()),
            expires_at: None,
        });

        if let RedisValue::List(list) = &mut entry.value {
            for element in elements {
                list.insert(0, element);
            }
            Ok(list.len())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }
    
    pub fn lpop(&mut self, key: &str) -> Result<Option<String>, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Ok(None);
            }

            if let RedisValue::List(list) = &mut entry.value {
                if list.is_empty() {
                    Ok(None)
                } else {
                    let element = list.remove(0);
                    if list.is_empty() {
                        self.map.remove(key);
                    }
                    Ok(Some(element))
                }
            } else {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
        } else {
            Ok(None)
        }
    }
    
    pub fn llen(&mut self, key: &str) -> Result<usize, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Ok(0);
            }

            match &entry.value {
                RedisValue::List(list) => Ok(list.len()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(0)
        }
    }
    
    pub fn lrange(&mut self, key: &str, start: isize, stop: isize) -> Result<Vec<String>, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Ok(vec![]);
            }

            if let RedisValue::List(list) = &entry.value {
                let len = list.len() as isize;
                let mut start = if start < 0 { len + start } else { start };
                let mut stop = if stop < 0 { len + stop } else { stop };
                
                start = std::cmp::max(0, start);
                stop = std::cmp::max(0, stop);

                if start >= len || start > stop {
                    return Ok(vec![]);
                }

                let end = std::cmp::min(stop as usize, list.len() - 1);
                Ok(list[start as usize..=end].to_vec())

            } else {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
        } else {
            Ok(vec![])
        }
    }
}