use crate::commands;
use crate::db::InMemoryDB;
use crate::notifier::Notifier;
use crate::propagator::CommandPropagator;
use crate::resp::*;
use crate::types::{Config, ServerState};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};

// In src/handler.rs

pub fn handle_client(
    mut stream: TcpStream,
    db_arc: Arc<Mutex<InMemoryDB>>,
    config: Arc<Config>,
    notifier: Arc<Mutex<Notifier>>,
    state_arc: Arc<Mutex<ServerState>>,
    propagator_arc: Arc<Mutex<CommandPropagator>>,
) {
    let mut buffer = [0; 1024];
    let mut in_transaction = false;
    let mut command_queue: Vec<Vec<String>> = Vec::new();

    loop {
        let n = match stream.read(&mut buffer) { Ok(0) => return, Ok(n) => n, Err(_) => return, };
        let input = String::from_utf8_lossy(&buffer[..n]);
        let args = parse_resp(&input);
        if args.is_empty() { continue; }
        let cmd = args[0].to_uppercase();

        if cmd == "PSYNC" {
            let state = state_arc.lock().unwrap();
            let mut propagator = propagator_arc.lock().unwrap();
            if let Err(e) = commands::handle_psync(&args, &state, &mut stream, &mut propagator) {
                eprintln!("Error during PSYNC, closing replica connection: {}", e);
            }
            return;
        }

        let (response, is_write_cmd) = match cmd.as_str() {
            "BLPOP" => (commands::handle_blpop(&args, &db_arc, &notifier), false),
            "MULTI" => {
                if in_transaction { (encode_error("MULTI calls can not be nested"), false) } 
                else { in_transaction = true; command_queue.clear(); (encode_simple_string("OK"), false) }
            },
            "DISCARD" => {
                if !in_transaction { (encode_error("DISCARD without MULTI"), false) } 
                else { in_transaction = false; command_queue.clear(); (encode_simple_string("OK"), false) }
            },
            // In src/handler.rs

            "EXEC" => {
                if !in_transaction {
                    (encode_error("EXEC without MULTI"), false)
                } else {
                    in_transaction = false;
                    let mut response_parts = Vec::with_capacity(command_queue.len());
                    let mut commands_to_propagate = Vec::new();

                    // Phase 1: Execute commands and collect responses/commands for propagation
                    {
                        let mut db = db_arc.lock().unwrap();
                        for queued_args in &command_queue {
                            let queued_cmd = queued_args[0].to_uppercase();
                            let (response_part, is_write) = match queued_cmd.as_str() {
                                "PING" => (commands::handle_ping(queued_args), false),
                                "ECHO" => (commands::handle_echo(queued_args), false),
                                "GET" => (commands::handle_get(queued_args, &mut db), false),
                                "LLEN" => (commands::handle_llen(queued_args, &mut db), false),
                                "LRANGE" => (commands::handle_lrange(queued_args, &mut db), false),
                                "SET" => (commands::handle_set(queued_args, &mut db), true),
                                "INCR" => (commands::handle_incr(queued_args, &mut db), true),
                                "LPUSH" => (commands::handle_lpush(queued_args, &mut db, &notifier), true),
                                "RPUSH" => (commands::handle_rpush(queued_args, &mut db, &notifier), true),
                                "LPOP" => (commands::handle_lpop(queued_args, &mut db), true),
                                _ => (encode_error("command not allowed in transaction"), false),
                            };
                            
                            if is_write && !response_part.starts_with("-") {
                                 commands_to_propagate.push(encode_array(queued_args));
                            }
                            response_parts.push(response_part);
                        }
                    } // The database lock is released here

                    // Phase 2: Propagate commands
                    if !commands_to_propagate.is_empty() {
                        let mut propagator = propagator_arc.lock().unwrap();
                        for command_str in commands_to_propagate {
                            propagator.propagate(command_str);
                        }
                    }
                    
                    command_queue.clear();
                    
                    // Phase 3: Format the final response
                    let mut final_response = format!("*{}\r\n", response_parts.len());
                    for part in response_parts {
                        final_response.push_str(&part);
                    }
                    (final_response, false)
                }
            },
            _ => {
                if in_transaction {
                    match cmd.as_str() {
                        "MULTI" | "EXEC" | "DISCARD" => (encode_error(&format!("{} command not allowed", cmd)), false),
                        _ => { command_queue.push(args.clone()); (encode_simple_string("QUEUED"), false) }
                    }
                } else {
                    let mut db = db_arc.lock().unwrap();
                    let state = state_arc.lock().unwrap();
                    
                    let (resp, is_write) = match cmd.as_str() {
                        // Read Commands
                        "PING" | "ECHO" | "REPLCONF" | "GET" | "INFO" | "CONFIG" | "KEYS" | "LLEN" | "LRANGE" => {
                            let r = match cmd.as_str() {
                                "PING" => commands::handle_ping(&args),
                                "ECHO" => commands::handle_echo(&args),
                                "REPLCONF" => commands::handle_replconf(&args),
                                "GET" => commands::handle_get(&args, &mut db),
                                "INFO" => commands::handle_info(&args, &state),
                                "CONFIG" => commands::handle_config(&args, &config),
                                "KEYS" => commands::handle_keys(&args, &mut db),
                                "LLEN" => commands::handle_llen(&args, &mut db),
                                "LRANGE" => commands::handle_lrange(&args, &mut db),
                                _ => unreachable!(),
                            };
                            (r, false)
                        },
                        // Write Commands
                        "SET" | "INCR" | "LPUSH" | "RPUSH" | "LPOP" => {
                             let r = match cmd.as_str() {
                                "SET" => commands::handle_set(&args, &mut db),
                                "INCR" => commands::handle_incr(&args, &mut db),
                                "LPUSH" => commands::handle_lpush(&args, &mut db, &notifier),
                                "RPUSH" => commands::handle_rpush(&args, &mut db, &notifier),
                                "LPOP" => commands::handle_lpop(&args, &mut db),
                                _ => unreachable!(),
                            };
                            (r, true)
                        },
                        _ => (encode_error("Unknown command"), false),
                    };
                    (resp, is_write)
                }
            }
        };

        stream.write_all(response.as_bytes()).unwrap();

        if is_write_cmd && !response.starts_with("-") {
            let mut propagator = propagator_arc.lock().unwrap();
            propagator.propagate(encode_array(&args));
        }
    }
}