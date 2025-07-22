mod db;
mod handler;
mod rdb;
mod resp;

use db::InMemoryDB;
use handler::handle_client;
use rdb::{load_db_from_rdb,}; // Import RdbEntry
//use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct Config {
    dir: String,
    dbfilename: String,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut dir = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                i += 1;
                if i < args.len() {
                    dir = args[i].clone();
                }
            }
            "--dbfilename" => {
                i += 1;
                if i < args.len() {
                    dbfilename = args[i].clone();
                }
            }
            _ => {}
        }
        i += 1;
    }

    let config = Arc::new(Config {
        dir: dir.clone(),
        dbfilename: dbfilename.clone(),
    });

    let db = Arc::new(Mutex::new(InMemoryDB::new()));

    
    // In main.rs

    let rdb_path = Path::new(&dir).join(&dbfilename);
    match load_db_from_rdb(&rdb_path) {
        Ok(data) => {
            if !data.is_empty() {
                println!("Loaded {} keys from RDB file.", data.len());
                let mut db_locked = db.lock().unwrap();
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
            // **ADD THIS LINE:**
            std::process::exit(1);
        }
    }

// ... rest of the main function
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on port 6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db_clone = Arc::clone(&db);
                let config_clone = Arc::clone(&config);
                std::thread::spawn(move || handle_client(stream, db_clone, config_clone));
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}



