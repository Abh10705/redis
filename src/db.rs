use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

pub struct InMemoryDB {
    map: HashMap<String, Entry>,
}

impl InMemoryDB {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(
            key,
            Entry {
                value,
                expires_at: None,
            },
        );
    }

    // Add this function back inside the `impl InMemoryDB` block in db.rs

    pub fn set_with_expiry(&mut self, key: String, value: String, ttl_ms: u64) {
        self.map.insert(
            key,
            Entry {
                value,
                expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)),
            },
        );
    }// This is the new method we are adding
    pub fn set_with_absolute_expiry(&mut self, key: String, value: String, expiry_ms: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Calculate the TTL from now. If it's already in the past, TTL will be 0.
        let ttl_ms = expiry_ms.saturating_sub(now_ms);

        self.map.insert(
            key,
            Entry {
                value,
                expires_at: Some(Instant::now() + Duration::from_millis(ttl_ms)),
            },
        );
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(entry) = self.map.get(key) {
            if let Some(expiry) = entry.expires_at {
                if expiry <= Instant::now() {
                    self.map.remove(key);
                    return None;
                }
            }
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn keys(&mut self) -> Vec<String> {
        self.map.retain(|_, v| {
            v.expires_at.map(|t| t > Instant::now()).unwrap_or(true)
        });
        self.map.keys().cloned().collect()
    }
}