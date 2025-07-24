use crate::commands;
use crate::db::InMemoryDB;
use crate::notifier::Notifier;
use crate::resp::*;
use crate::Config;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

pub fn handle_client(
    mut stream: TcpStream,
    db_arc: Arc<Mutex<InMemoryDB>>,
    config: Arc<Config>,
    notifier: Arc<Mutex<Notifier>>,
) {
    let mut buffer = [0; 512];
    // **NEW:** A state flag for the current connection.
    let mut in_transaction = false;

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
            // **MODIFIED:** MULTI and EXEC are now handled here directly.
            "MULTI" => {
                in_transaction = true;
                encode_simple_string("OK")
            }
            "EXEC" => {
                if in_transaction {
                    in_transaction = false; // Reset the state
                    encode_array(&[]) // Return an empty array
                } else {
                    encode_error("EXEC without MULTI")
                }
            }
            "BLPOP" => {
                if args.len() != 3 {
                    encode_error("wrong number of arguments for 'blpop' command")
                } else {
                    let key = args[1].clone();
                    // **MODIFIED:** Parse the timeout argument
                    let timeout_res = args[2].parse::<f64>();

                    if timeout_res.is_err() {
                        encode_error("timeout is not a float or out of range")
                    } else {
                        let timeout = Duration::from_secs_f64(timeout_res.unwrap());
                        loop {
                            let mut db_lock = db_arc.lock().unwrap();
                            if let Ok(Some(element)) = db_lock.lpop(&key) {
                                let items = vec![key, element];
                                break encode_array(&items);
                            }

                            // If timeout is 0, we can block "forever".
                            // For simplicity in this stage, we handle non-zero timeouts.
                            if timeout.as_secs() == 0 && timeout.subsec_nanos() == 0 {
                                // This logic is for the previous stage, but we keep it
                                let (tx, rx) = mpsc::channel();
                                notifier.lock().unwrap().add_waiter(key.clone(), tx);
                                drop(db_lock);
                                let _ = rx.recv(); // Block forever
                                continue; // Loop again to retry the pop
                            }
                            
                            // **MODIFIED:** Block with a timeout
                            let (tx, rx) = mpsc::channel();
                            notifier.lock().unwrap().add_waiter(key.clone(), tx);
                            drop(db_lock);
                            
                            match rx.recv_timeout(timeout) {
                                Ok(_) => {
                                    // Woken up by a push, loop again to retry pop
                                    continue;
                                }
                                Err(_) => {
                                    // Timeout reached, break and return null
                                    break encode_null_bulk_string();
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                let mut db = db_arc.lock().unwrap();
                match cmd.as_str() {
                    "PING" => commands::handle_ping(&args),
                    "ECHO" => commands::handle_echo(&args),
                    "MULTI" => encode_simple_string("OK"),
                    "EXEC" => encode_error("EXEC without MULTI"),
                    "SET" => commands::handle_set(&args, &mut db),
                    "GET" => commands::handle_get(&args, &mut db),
                    "INCR" => commands::handle_incr(&args, &mut db),
                    "CONFIG" => commands::handle_config(&args, &config),
                    "KEYS" => commands::handle_keys(&args, &mut db),
                    "LPUSH" => commands::handle_lpush(&args, &mut db, &notifier),
                    "RPUSH" => commands::handle_rpush(&args, &mut db, &notifier),
                    "LPOP" => commands::handle_lpop(&args, &mut db),
                    "LLEN" => commands::handle_llen(&args, &mut db),
                    "LRANGE" => commands::handle_lrange(&args, &mut db),
                    _ => encode_error("Unknown command"),
                }
            }
        };
        stream.write_all(response.as_bytes()).unwrap();
    }
}