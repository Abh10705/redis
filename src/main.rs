mod commands;
mod db;
mod handler;
mod lists;
mod notifier;
mod rdb;
mod resp;

use db::InMemoryDB;
use handler::handle_client;
use notifier::Notifier;
use rdb::load_db_from_rdb;
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct Config {
    dir: String,
    dbfilename: String,
}

pub struct ServerState {
    pub role: String,
    pub master_replid: String,
    pub master_repl_offset: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port = 6379;
    let mut role = "master".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                if i < args.len() {
                    port = args[i].parse::<u16>().unwrap_or(6379);
                }
            }
            "--replicaof" => {
                role = "slave".to_string();
                i += 2;
            }
            "--dir" => { /* ... unchanged ... */ },
            "--dbfilename" => { /* ... unchanged ... */ },
            _ => { i += 1; }
        }
    }

    let config = Arc::new(Config {
        dir: dir.clone(),
        dbfilename: dbfilename.clone(),
    });
    let server_state = Arc::new(Mutex::new(ServerState { role }));
    let db_arc = Arc::new(Mutex::new(InMemoryDB::new()));
    let notifier_arc = Arc::new(Mutex::new(Notifier::new()));

    let rdb_path = Path::new(&dir).join(&dbfilename);
    match load_db_from_rdb(&rdb_path) {
        Ok(data) => {
            if !data.is_empty() {
                let mut db_locked = db_arc.lock().unwrap();
                for (key, entry) in data {
                    if let Some(expiry_ms) = entry.expiry_ms {
                        db_locked.set_with_absolute_expiry(key, entry.value, expiry_ms);
                    } else {
                        db_locked.set(key, entry.value);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error loading RDB file: {}", e);
            std::process::exit(1);
        }
    }

    let listener_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&listener_address).unwrap();
    println!("Listening on {}", listener_address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db_clone = Arc::clone(&db_arc);
                let config_clone = Arc::clone(&config);
                let notifier_clone = Arc::clone(&notifier_arc);
                let state_clone = Arc::clone(&server_state);
                std::thread::spawn(move || {
                    handle_client(stream, db_clone, config_clone, notifier_clone, state_clone)
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}