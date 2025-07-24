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
    let mut in_transaction = false;
    let mut command_queue: Vec<Vec<String>> = Vec::new();

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
            "MULTI" => {
                if in_transaction {
                    encode_error("MULTI calls can not be nested")
                } else {
                    in_transaction = true;
                    command_queue.clear();
                    encode_simple_string("OK")
                }
            }
            "EXEC" => {
                if !in_transaction {
                    encode_error("EXEC without MULTI")
                } else {
                    in_transaction = false;
                    let mut response_parts = Vec::with_capacity(command_queue.len());
                    let mut db = db_arc.lock().unwrap();
                    
                    for queued_args in &command_queue {
                        let queued_cmd = queued_args[0].to_uppercase();
                        let response_part = match queued_cmd.as_str() {
                            "PING" => commands::handle_ping(queued_args),
                            "ECHO" => commands::handle_echo(queued_args),
                            "SET" => commands::handle_set(queued_args, &mut db),
                            "GET" => commands::handle_get(queued_args, &mut db),
                            "INCR" => commands::handle_incr(queued_args, &mut db),
                            "CONFIG" => commands::handle_config(queued_args, &config),
                            "KEYS" => commands::handle_keys(queued_args, &mut db),
                            "LPUSH" => commands::handle_lpush(queued_args, &mut db, &notifier),
                            "RPUSH" => commands::handle_rpush(queued_args, &mut db, &notifier),
                            "LPOP" => commands::handle_lpop(queued_args, &mut db),
                            "LLEN" => commands::handle_llen(queued_args, &mut db),
                            "LRANGE" => commands::handle_lrange(queued_args, &mut db),
                            _ => encode_error("Unknown command in transaction"),
                        };
                        response_parts.push(response_part);
                    }
                    command_queue.clear();
                    
                    // **THE FIX:** Manually build the array from pre-encoded parts.
                    let mut final_response = format!("*{}\r\n", response_parts.len());
                    for part in response_parts {
                        final_response.push_str(&part);
                    }
                    final_response
                }
            }
            "BLPOP" => {
                // BLPOP is a special case and is not queued in a transaction.
                if args.len() != 3 {
                    encode_error("wrong number of arguments for 'blpop' command")
                } else {
                    let key = args[1].clone();
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

                            if timeout.as_secs() == 0 && timeout.subsec_nanos() == 0 {
                                let (tx, rx) = mpsc::channel();
                                notifier.lock().unwrap().add_waiter(key.clone(), tx);
                                drop(db_lock);
                                let _ = rx.recv();
                                continue;
                            }
                            
                            let (tx, rx) = mpsc::channel();
                            notifier.lock().unwrap().add_waiter(key.clone(), tx);
                            drop(db_lock);
                            
                            match rx.recv_timeout(timeout) {
                                Ok(_) => continue,
                                Err(_) => break encode_null_bulk_string(),
                            }
                        }
                    }
                }
            }
            // Handle all other commands
            _ => {
                if in_transaction {
                    // If we are in a transaction, queue the command and reply "QUEUED".
                    command_queue.push(args);
                    encode_simple_string("QUEUED")
                } else {
                    // Otherwise, execute the command immediately.
                    let mut db = db_arc.lock().unwrap();
                    match cmd.as_str() {
                        "PING" => commands::handle_ping(&args),
                        "ECHO" => commands::handle_echo(&args),
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
            }
        };
        stream.write_all(response.as_bytes()).unwrap();
    }
}