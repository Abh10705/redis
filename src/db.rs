use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
enum RedisValue {
    String(String),
    List(Vec<String>),
}

#[derive(Clone)]
struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
}

pub struct InMemoryDB {
    map: HashMap<String, Entry>,
}

impl InMemoryDB {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    pub fn set(&mut self, key: String, value: String) {
        let entry = Entry { value: RedisValue::String(value), expires_at: None };
        self.map.insert(key, entry);
    }
    
    pub fn set_with_expiry(&mut self, key: String, value: String, ttl_ms: u64) {
        let entry = Entry { value: RedisValue::String(value), expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)) };
        self.map.insert(key, entry);
    }

    pub fn set_with_absolute_expiry(&mut self, key: String, value: String, expiry_ms: u64) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let ttl_ms = expiry_ms.saturating_sub(now_ms);
        let entry = Entry { value: RedisValue::String(value), expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)) };
        self.map.insert(key, entry);
    }
    
    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return None;
            }
            if let RedisValue::String(s) = &entry.value {
                return Some(s.clone());
            }
        }
        None
    }
    
    pub fn keys(&mut self) -> Vec<String> {
        self.map.retain(|_, v| v.expires_at.map_or(true, |t| t > Instant::now()));
        self.map.keys().cloned().collect()
    }

    pub fn rpush(&mut self, key: String, elements: Vec<String>) -> Result<usize, &'static str> {
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


    pub fn lrange(&mut self, key: &str, start: isize, stop: isize) -> Result<Vec<String>, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Ok(vec![]);
            }

            if let RedisValue::List(list) = &entry.value {
                let len = list.len() as isize;

                // **NEW:** Convert start and stop indexes
                let mut start = if start < 0 { len + start } else { start };
                let mut stop = if stop < 0 { len + stop } else { stop };
                
                // Clamp to bounds (if negative index was too large, it becomes 0)
                start = std::cmp::max(0, start);
                stop = std::cmp::max(0, stop);

                // Handle edge cases with the now-positive indexes
                if start >= len || start > stop {
                    return Ok(vec![]);
                }

                // Adjust stop index and slice
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
    // In src/db.rs, inside `impl InMemoryDB