use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// A new enum to represent the different types of values Redis can store.
#[derive(Clone)]
enum RedisValue {
    String(String),
    List(Vec<String>),
}

#[derive(Clone)]
struct Entry {
    value: RedisValue, // The value is now this enum
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
        let entry = Entry {
            value: RedisValue::String(value), // Wrap the String in our enum
            expires_at: None,
        };
        self.map.insert(key, entry);
    }

    pub fn set_with_expiry(&mut self, key: String, value: String, ttl_ms: u64) {
        let entry = Entry {
            value: RedisValue::String(value),
            expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)),
        };
        self.map.insert(key, entry);
    }

    pub fn set_with_absolute_expiry(&mut self, key: String, value: String, expiry_ms: u64) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let ttl_ms = expiry_ms.saturating_sub(now_ms);
        let entry = Entry {
            value: RedisValue::String(value),
            expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)),
        };
        self.map.insert(key, entry);
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(entry) = self.map.get_mut(key) {
            if let Some(expiry) = entry.expires_at {
                if expiry <= Instant::now() {
                    self.map.remove(key);
                    return None;
                }
            }
            // Now we must check if the value is actually a String
            if let RedisValue::String(s) = &entry.value {
                return Some(s.clone());
            }
        }
        None
    }

    // The new method for RPUSH
    pub fn rpush(&mut self, key: String, element: String) -> usize {
        // For this stage, we assume the list is new.
        let list = vec![element];
        let new_len = list.len();
        let entry = Entry {
            value: RedisValue::List(list),
            expires_at: None,
        };
        self.map.insert(key, entry);
        new_len // Return the length of the new list
    }

    pub fn keys(&mut self) -> Vec<String> {
        self.map.retain(|_, v| v.expires_at.map_or(true, |t| t > Instant::now()));
        self.map.keys().cloned().collect()
    }
}