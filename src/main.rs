mod resp;
mod db;
mod rdb;
mod handler;

use std::{
    env,
    net::TcpListener,
    path::Path,
    sync::{Arc, Mutex},
};

use db::InMemoryDB;
use rdb::load_keys_from_rdb;
use handler::handle_client;

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

    let db = Arc::new(Mutex::new(InMemoryDB::new()));

    // Load keys from RDB if file exists
        // In main.rs
// Load keys from RDB if file exists
let rdb_path = Path::new(&dir).join(&dbfilename);
match load_keys_from_rdb(&rdb_path) {
    Ok(keys) => {
        if !keys.is_empty() {
            println!("Loaded {} keys from RDB file.", keys.len());
            let mut db_locked = db.lock().unwrap();
            for key in keys {
                db_locked.set(key, "(loaded-from-rdb)".to_string());
            }
        }
    }
    Err(e) => {
        // THIS IS THE IMPORTANT PART
        eprintln!("Error loading RDB file: {}", e);
    }
}

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on port 6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || handle_client(stream, db));
            }
            Err(e) => eprintln!("Connection error: {}", e),
        }
    }
}
