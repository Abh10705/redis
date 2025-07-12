use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod resp;
use resp::parse_resp;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis clone listening on 127.0.0.1:6379");

    // Database: key -> (value, optional expiry time)
    let db: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        let db = Arc::clone(&db);

        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    let mut buf = [0; 512];

                    loop {
                        let read_count = match stream.read(&mut buf) {
                            Ok(0) => break, // connection closed
                            Ok(n) => n,
                            Err(_) => break,
                        };

                        let input = String::from_utf8_lossy(&buf[..read_count]);
                        let args = parse_resp(&input);

                        if args.is_empty() {
                            let _ = stream.write_all(b"-ERR empty or invalid command\r\n");
                            continue;
                        }

                        match args[0].to_uppercase().as_str() {
                            "PING" => {
                                let _ = stream.write_all(b"+PONG\r\n");
                            }

                            "ECHO" => {
                                if args.len() < 2 {
                                    let _ = stream.write_all(b"-ERR missing argument for ECHO\r\n");
                                } else {
                                    let msg = &args[1];
                                    let response = format!("${}\r\n{}\r\n", msg.len(), msg);
                                    let _ = stream.write_all(response.as_bytes());
                                }
                            }

                            "SET" => {
                                if args.len() < 3 {
                                    let _ = stream.write_all(
                                        b"-ERR wrong number of arguments for 'SET'\r\n",
                                    );
                                } else {
                                    let key = &args[1];
                                    let value = &args[2];
                                    let mut expiry: Option<Instant> = None;

                                    if args.len() >= 5 && args[3].to_uppercase() == "PX" {
                                        if let Ok(ms) = args[4].parse::<u64>() {
                                            expiry = Some(Instant::now() + Duration::from_millis(ms));
                                        }
                                    }

                                    let mut store = db.lock().unwrap();
                                    store.insert(key.clone(), (value.clone(), expiry));

                                    let _ = stream.write_all(b"+OK\r\n");
                                }
                            }

                            "GET" => {
                                if args.len() != 2 {
                                    let _ = stream.write_all(
                                        b"-ERR wrong number of arguments for 'GET'\r\n",
                                    );
                                } else {
                                    let key = &args[1];

                                    let mut store = db.lock().unwrap();
                                    if let Some((value, expiry)) = store.get(key) {
                                        let expired = expiry
                                            .map_or(false, |e| Instant::now() > e);

                                        if expired {
                                            store.remove(key);
                                            let _ = stream.write_all(b"$-1\r\n");
                                        } else {
                                            let response = format!(
                                                "${}\r\n{}\r\n",
                                                value.len(),
                                                value
                                            );
                                            let _ = stream.write_all(response.as_bytes());
                                        }
                                    } else {
                                        let _ = stream.write_all(b"$-1\r\n");
                                    }
                                }
                            }

                            _ => {
                                let _ = stream.write_all(b"-ERR unknown command\r\n");
                            }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}
