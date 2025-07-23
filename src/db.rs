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

// **THE FIX:** Ensure all functions are inside this `impl` block
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
}