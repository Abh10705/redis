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
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Config {
    pub dir: String,
    pub dbfilename: String,
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
    let mut master_addr: Option<String> = None;

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
                i += 1;
                if i < args.len() {
                    let parts: Vec<&str> = args[i].split_whitespace().collect();
                    if parts.len() == 2 {
                        master_addr = Some(format!("{}:{}", parts[0], parts[1]));
                    }
                }
            }
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

    if let Some(addr) = master_addr {
        let replica_port = port;
        thread::spawn(move || {
            println!("Connecting to master at {}", addr);
            if let Ok(mut stream) = TcpStream::connect(addr) {
                println!("Connected to master. Sending PING.");
                let mut buffer = [0; 512];
               
                let ping_cmd = resp::encode_array(&["PING".to_string()]);
                stream.write_all(ping_cmd.as_bytes()).unwrap();
                stream.read(&mut buffer).unwrap(); // Read the +PONG

                // 2. Send REPLCONF listening-port
                let replconf_port_cmd = resp::encode_array(&[
                    "REPLCONF".to_string(),
                    "listening-port".to_string(),
                    replica_port.to_string(),
                ]);
                stream.write_all(replconf_port_cmd.as_bytes()).unwrap();
                stream.read(&mut buffer).unwrap(); // Read the +OK

                // 3. Send REPLCONF capa psync2
                let replconf_capa_cmd = resp::encode_array(&[
                    "REPLCONF".to_string(),
                    "capa".to_string(),
                    "psync2".to_string(),
                ]);
                stream.write_all(replconf_capa_cmd.as_bytes()).unwrap();
                stream.read(&mut buffer).unwrap(); // Read the +OK
                
                println!("Finished handshake with master.");

            } else {
                eprintln!("Failed to connect to master.");
            }
        });
    }

    let config = Arc::new(Config {
        dir: dir.clone(),
        dbfilename: dbfilename.clone(),
    });
    let server_state = Arc::new(Mutex::new(ServerState {
        role,
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: 0,
    }));
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
                thread::spawn(move || {
                    handle_client(stream, db_clone, config_clone, notifier_clone, state_clone)
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}