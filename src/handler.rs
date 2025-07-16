use std::net::TcpStream;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use crate::Config;
use crate::resp::*;
use crate::db::InMemoryDB;


pub fn handle_client(mut stream: TcpStream, db: Arc<Mutex<InMemoryDB>>, config: Arc<Config>) {
    let mut buffer = [0; 512];

    loop {
        let n = match stream.read(&mut buffer) {
            Ok(0) => return, // client disconnected
            Ok(n) => n,
            Err(_) => return,
        };

        let input = String::from_utf8_lossy(&buffer[..n]);
        let args = parse_resp(&input);

        if args.is_empty() {
            continue;
        }

        let cmd = args[0].to_uppercase();

        let mut db = db.lock().unwrap();

        let response = match cmd.as_str() {
            "PING" => encode_simple_string("PONG"),

            "ECHO" => {
                if args.len() >= 2 {
                    encode_bulk_string(&args[1])
                } else {
                    encode_error("ECHO needs one argument")
                }
            }

            "SET" => {
                if args.len() < 3 {
                    encode_error("SET needs key and value")
                } else {
                    let key = args[1].clone();
                    let value = args[2].clone();
                    if args.len() >= 5 && args[3].to_uppercase() == "PX" {
                        if let Ok(ms) = args[4].parse::<u64>() {
                            db.set_with_expiry(key, value, ms);
                        } else {
                            return;
                        }
                    } else {
                        db.set(key, value);
                    }
                    encode_simple_string("OK")
                }
            }

            "GET" => {
                if args.len() < 2 {
                    encode_error("GET needs key")
                } else {
                    match db.get(&args[1]) {
                        Some(val) => encode_bulk_string(&val),
                        None => encode_null_bulk_string(),
                    }
                }
            }
            "CONFIG" => {
        if args.len() < 3 || args[1].to_uppercase() != "GET" {
            encode_error("Only CONFIG GET is supported")
        } else {
            let key = &args[2];
            match key.as_str() {
                "dir" => {
                    let items = vec!["dir".to_string(), config.dir.clone()];
                    encode_array(&items)
                }
                "dbfilename" => {
                    let items = vec!["dbfilename".to_string(), config.dbfilename.clone()];
                    encode_array(&items)
                }
                _ => encode_error("Unknown CONFIG key")
            }
        }
    }

            "KEYS" => {
                if args.len() == 2 && args[1] == "*" {
                    let keys = db.keys();
                    encode_array(&keys)
                } else {
                    encode_error("Only KEYS * is supported")
                }
            }

            _ => encode_error("Unknown command"),
        };

        stream.write_all(response.as_bytes()).unwrap();
    }
}
