use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
}

#[derive(Clone)]
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<Instant>,
}

// **FIX 1:** Make the struct itself public.
pub struct InMemoryDB {
    // **FIX 2:** Make the `map` field public *within the crate*.
    // This allows our `lists.rs` module to access it.
    pub(crate) map: HashMap<String, Entry>,
}

// This block now only contains the CORE, non-list methods.
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
    
    pub fn get(&mut self, key: &str) -> Result<Option<String>, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Ok(None);
            }

            match &entry.value {
                RedisValue::String(s) => Ok(Some(s.clone())),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(None)
        }
    }
    
    pub fn keys(&mut self) -> Vec<String> {
        self.map.retain(|_, v| v.expires_at.map_or(true, |t| t > Instant::now()));
        self.map.keys().cloned().collect()
    }
    // In src/db.rs

    pub fn incr(&mut self, key: &str) -> Result<i64, &'static str> {
        // Get the entry for the key. If it doesn't exist, insert a new entry
        // with a default value of "0" and no expiry.
        let entry = self.map.entry(key.to_string()).or_insert_with(|| Entry {
            value: RedisValue::String("0".to_string()),
            expires_at: None,
        });

        // Handle expired keys: if an expired key was found, treat it as a new key.
        if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
            entry.value = RedisValue::String("0".to_string());
            entry.expires_at = None;
        }

        if let RedisValue::String(s) = &mut entry.value {
            match s.parse::<i64>() {
                Ok(mut num) => {
                    num += 1;
                    *s = num.to_string(); // Update the string value in place
                    Ok(num)
                }
                Err(_) => Err("value is not an integer or out of range"),
            }
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }
}