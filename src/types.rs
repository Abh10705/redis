use std::time::Instant;

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

pub struct Config {
    pub dir: String,
    pub dbfilename: String,
}

pub struct ServerState {
    pub role: String,
    pub master_replid: String,
    pub master_repl_offset: usize,
}

pub struct RdbEntry {
    pub value: String,
    pub expiry_ms: Option<u64>,
}