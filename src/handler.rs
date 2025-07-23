use crate::db::InMemoryDB;
use crate::notifier::Notifier;
use crate::resp::*;
use crate::Config;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};

pub fn handle_client(
    mut stream: TcpStream,
    db_arc: Arc<Mutex<InMemoryDB>>,
    config: Arc<Config>,
    notifier: Arc<Mutex<Notifier>>,
) {
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

        let response = match cmd.as_str() {
            "BLPOP" => {
                if args.len() != 3 {
                    encode_error("wrong number of arguments for 'blpop' command")
                } else {
                    let key = args[1].clone();
                    // Loop until we can pop an element
                    loop {
                        let mut db_lock = db_arc.lock().unwrap();
                        // Try to pop an element non-blockingly first
                        if let Ok(Some(element)) = db_lock.lpop(&key) {
                            let items = vec![key, element];
                            break encode_array(&items);
                        }

                        // If the list was empty, we need to block.
                        let (tx, rx) = mpsc::channel();
                        notifier.lock().unwrap().add_waiter(key.clone(), tx);
                        
                        // Release the database lock so other clients can push
                        drop(db_lock); 
                        
                        // Wait for a signal.
                        // We don't care about the result, it's just a wakeup call.
                        let _ = rx.recv();
                    }
                }
            }
            _ => {
                let mut db = db_arc.lock().unwrap();
                match cmd.as_str() {
                    "PING" => encode_simple_string("PONG"),
                    "ECHO" => {
                        if args.len() < 2 {
                            encode_error("ECHO needs one argument")
                        } else {
                            encode_bulk_string(&args[1])
                        }
                    },
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
                    },
                    "GET" => {
                        if args.len() < 2 {
                            encode_error("wrong number of arguments for 'get' command")
                        } else {
                            match db.get(&args[1]) {
                                Some(val) => encode_bulk_string(&val),
                                None => encode_null_bulk_string(),
                            }
                        }
                    },
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
                    },
                    "KEYS" => {
                        if args.len() == 2 && args[1] == "*" {
                            let keys = db.keys();
                            encode_array(&keys)
                        } else {
                            encode_error("Only KEYS * is supported")
                        }
                    },
                    "LPUSH" => {
                        if args.len() < 3 {
                            encode_error("wrong number of arguments for 'lpush' command")
                        } else {
                            let key = args[1].clone();
                            let elements: Vec<String> = args[2..].to_vec();
                            match db.lpush(key, elements, &notifier) {
                                Ok(list_len) => encode_integer(list_len as i64),
                                Err(msg) => encode_error(msg),
                            }
                        }
                    },
                    "RPUSH" => {
                        if args.len() < 3 {
                            encode_error("wrong number of arguments for 'rpush' command")
                        } else {
                            let key = args[1].clone();
                            let elements: Vec<String> = args[2..].to_vec();
                            match db.rpush(key, elements, &notifier) {
                                Ok(list_len) => encode_integer(list_len as i64),
                                Err(msg) => encode_error(msg),
                            }
                        }
                    },
                    "LPOP" => {
                        if args.len() < 2 || args.len() > 3 {
                            encode_error("wrong number of arguments for 'lpop' command")
                        } else if args.len() == 2 {
                            let key = &args[1];
                            match db.lpop(key) {
                                Ok(Some(element)) => encode_bulk_string(&element),
                                Ok(None) => encode_null_bulk_string(),
                                Err(msg) => encode_error(msg),
                            }
                        } else {
                            let key = &args[1];
                            match args[2].parse::<usize>() {
                                Ok(count) => match db.lpop_count(key, count) {
                                    Ok(elements) => encode_array(&elements),
                                    Err(msg) => encode_error(msg),
                                },
                                Err(_) => encode_error("value is not an integer or out of range"),
                            }
                        }
                    },
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
                    },
                    "LRANGE" => {
                        if args.len() != 4 {
                            encode_error("wrong number of arguments for 'lrange' command")
                        } else {
                            let key = &args[1];
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
                    },
                    _ => encode_error("Unknown command"),
                }
            }
        };
        stream.write_all(response.as_bytes()).unwrap();
    }
}