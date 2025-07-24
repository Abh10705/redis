mod db;
mod handler;
mod lists;
mod notifier;
mod rdb;
mod resp;
mod commands;

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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => { /* ... unchanged ... */ },
            "--dbfilename" => { /* ... unchanged ... */ },
            _ => {}
        }
        i += 1;
    }

    let config = Arc::new(Config {
        dir: dir.clone(),
        dbfilename: dbfilename.clone(),
    });

    let db_arc = Arc::new(Mutex::new(InMemoryDB::new()));
    // **THE FIX:** Renamed `notifier` to `notifier_arc` to avoid conflict with the module name.
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

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on port 6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db_clone = Arc::clone(&db_arc);
                let config_clone = Arc::clone(&config);
                let notifier_clone = Arc::clone(&notifier_arc);
                std::thread::spawn(move || {
                    handle_client(stream, db_clone, config_clone, notifier_clone)
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}