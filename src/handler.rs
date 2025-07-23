use crate::db::InMemoryDB;
use crate::resp::*;
use crate::Config;
use crate::rdb::RdbEntry; //
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub fn handle_client(mut stream: TcpStream, db: Arc<Mutex<InMemoryDB>>, config: Arc<Config>) {
    let mut buffer = [0; 512];

    loop {
        let n = match stream.read(&mut buffer) {
            Ok(0) => return,
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
            // **FIX 2:** Rewritten SET logic to correctly handle all cases.
            "SET" => {
                if args.len() < 3 {
                    encode_error("wrong number of arguments for 'set' command")
                } else if args.len() >= 5 && args[3].to_uppercase() == "PX" {
                    let key = args[1].clone();
                    let value = args[2].clone();
                    match args[4].parse::<u64>() {
                        Ok(ms) => {
                            db.set_with_expiry(key, value, ms);
                            encode_simple_string("OK")
                        }
                        Err(_) => {
                            encode_error("value is not an integer or out of range")
                        }
                    }
                } else {
                    let key = args[1].clone();
                    let value = args[2].clone();
                    db.set(key, value);
                    encode_simple_string("OK")
                }
            }
            "GET" => {
                if args.len() < 2 {
                    encode_error("wrong number of arguments for 'get' command")
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
                        "dir" => encode_array(&["dir".to_string(), config.dir.clone()]),
                        "dbfilename" => {
                            encode_array(&["dbfilename".to_string(), config.dbfilename.clone()])
                        }
                        _ => encode_error("Unknown CONFIG key"),
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
           // In src/handler.rs, inside the `handle_client` function's loop

// ... inside the `let response = match ...` block

    // ADD THIS NEW MATCH ARM (usually placed before RPUSH)
            "LPUSH" => {
                if args.len() < 3 {
                    encode_error("wrong number of arguments for 'lpush' command")
                } else {
                    let key = args[1].clone();
                    let elements: Vec<String> = args[2..].to_vec();
                    match db.lpush(key, elements) {
                        Ok(list_len) => encode_integer(list_len as i64),
                        Err(msg) => encode_error(msg),
                    }
                }
            }

           
            "RPUSH" => {
                if args.len() < 3 {
                    encode_error("wrong number of arguments for 'rpush' command")
                } else {
                    let key = args[1].clone();
                    let elements: Vec<String> = args[2..].to_vec();
                    match db.rpush(key, elements) {
                        Ok(list_len) => encode_integer(list_len as i64),
                        Err(msg) => encode_error(msg),
                    }
                }
            }
            
            // ADD THIS NEW MATCH ARM
            "LLEN" => {
                if args.len() != 2 {
                    encode_error("wrong number of arguments for 'llen' command")
                } else {
                    let key = &args[1];
                    match db.llen(key) {
                        Ok(list_len) => encode_integer(list_len as i64),
                        Err(msg) => encode_error(msg),
                    }
                }
            }

        // In src/handler.rs

            "LPOP" => {
                if args.len() < 2 || args.len() > 3 {
                    encode_error("wrong number of arguments for 'lpop' command")
                } else if args.len() == 2 {
                    // This is the original LPOP behavior for a single element
                    let key = &args[1];
                    match db.lpop(key) {
                        Ok(Some(element)) => encode_bulk_string(&element),
                        Ok(None) => encode_null_bulk_string(),
                        Err(msg) => encode_error(msg),
                    }
                } else {
                    // This is the new behavior for `LPOP key count`
                    let key = &args[1];
                    match args[2].parse::<usize>() {
                        Ok(count) => match db.lpop_count(key, count) {
                            Ok(elements) => encode_array(&elements),
                            Err(msg) => encode_error(msg),
                        },
                        Err(_) => encode_error("value is not an integer or out of range"),
                    }
                }
            }


            "LRANGE" => {
                if args.len() != 4 {
                    encode_error("wrong number of arguments for 'lrange' command")
                } else {
                    let key = &args[1];
                    // **MODIFIED:** Parse as `isize` to allow negative numbers.
                    let start_res = args[2].parse::<isize>();
                    let stop_res = args[3].parse::<isize>();

                    if start_res.is_err() || stop_res.is_err() {
                        encode_error("value is not an integer or out of range")
                    } else {
                        let start = start_res.unwrap();
                        let stop = stop_res.unwrap();
                        match db.lrange(key, start, stop) {
                            Ok(elements) => encode_array(&elements),
                            Err(msg) => encode_error(msg),
                        }
                    }
                }
            }
            _ => encode_error("Unknown command"),
        };
        stream.write_all(response.as_bytes()).unwrap();
    }
}