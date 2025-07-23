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

    // `set`, `set_with_expiry`, and `set_with_absolute_expiry` are unchanged.
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
    
    // `get` and `keys` are also unchanged.
    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(entry) = self.map.get_mut(key) {
            if let Some(expiry) = entry.expires_at {
                if expiry <= Instant::now() {
                    self.map.remove(key);
                    return None;
                }
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

    // **MODIFIED:** The rpush method is now much smarter.
    pub fn rpush(&mut self, key: String, element: String) -> Result<usize, &'static str> {
        // Find the entry for the key. If it doesn't exist, create a new empty list.
        let entry = self.map.entry(key).or_insert_with(|| Entry {
            value: RedisValue::List(Vec::new()),
            expires_at: None,
        });

        // Now, check if the value is actually a list.
        if let RedisValue::List(list) = &mut entry.value {
            list.push(element);
            Ok(list.len())
        } else {
            // The key exists but holds a String, not a List.
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }
}