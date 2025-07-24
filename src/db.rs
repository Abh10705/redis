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

pub struct InMemoryDB {
    pub(crate) map: HashMap<String, Entry>,
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
    
    // **THIS IS THE CORRECTED GET METHOD**
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

    pub fn incr(&mut self, key: &str) -> Result<i64, &'static str> {
        if let Some(entry) = self.map.get_mut(key) {
            if entry.expires_at.map_or(false, |e| e <= Instant::now()) {
                self.map.remove(key);
                return Err("Key does not exist or is expired");
            }
            if let RedisValue::String(s) = &mut entry.value {
                match s.parse::<i64>() {
                    Ok(mut num) => {
                        num += 1;
                        *s = num.to_string();
                        Ok(num)
                    }
                    Err(_) => Err("value is not an integer or out of range"),
                }
            } else {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
        } else {
            Err("no such key")
        }
    }
}