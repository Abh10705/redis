use crate::db::InMemoryDB;
use crate::notifier::Notifier;
use crate::propagator::CommandPropagator;
use crate::resp::*;
use crate::types::{Config, ServerState};
use std::io::Write;
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

pub fn handle_ping(_args: &[String]) -> String {
    encode_simple_string("PONG")
}

pub fn handle_echo(args: &[String]) -> String {
    if args.len() < 2 {
        encode_error("wrong number of arguments for 'echo' command")
    } else {
        encode_bulk_string(&args[1])
    }
}

pub fn handle_get(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() != 2 {
        encode_error("wrong number of arguments for 'get' command")
    } else {
        match db.get(&args[1]) {
            Ok(Some(val)) => encode_bulk_string(&val),
            Ok(None) => encode_null_bulk_string(),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_set(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() < 3 {
        return encode_error("wrong number of arguments for 'set' command");
    } else if args.len() >= 5 && args[3].to_uppercase() == "PX" {
        let key = args[1].clone();
        let value = args[2].clone();
        if let Ok(ms) = args[4].parse::<u64>() {
            db.set_with_expiry(key, value, ms);
            return encode_simple_string("OK");
        } else {
            return encode_error("value is not an integer or out of range");
        }
    } else {
        let key = args[1].clone();
        let value = args[2].clone();
        db.set(key, value);
        return encode_simple_string("OK");
    }
}

pub fn handle_config(args: &[String], config: &Arc<Config>) -> String {
    if args.len() < 3 || args[1].to_uppercase() != "GET" {
        encode_error("Only CONFIG GET is supported")
    } else {
        match args[2].as_str() {
            "dir" => encode_array(&["dir".to_string(), config.dir.clone()]),
            "dbfilename" => encode_array(&["dbfilename".to_string(), config.dbfilename.clone()]),
            _ => encode_error("Unknown CONFIG key"),
        }
    }
}

pub fn handle_keys(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() == 2 && args[1] == "*" {
        encode_array(&db.keys())
    } else {
        encode_error("Only KEYS * is supported")
    }
}

pub fn handle_lpush(args: &[String], db: &mut InMemoryDB, notifier: &Arc<Mutex<Notifier>>) -> String {
    if args.len() < 3 {
        encode_error("wrong number of arguments for 'lpush' command")
    } else {
        let key = args[1].clone();
        let elements: Vec<String> = args[2..].to_vec();
        match db.lpush(key, elements, notifier) {
            Ok(list_len) => encode_integer(list_len as i64),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_rpush(args: &[String], db: &mut InMemoryDB, notifier: &Arc<Mutex<Notifier>>) -> String {
    if args.len() < 3 {
        encode_error("wrong number of arguments for 'rpush' command")
    } else {
        let key = args[1].clone();
        let elements: Vec<String> = args[2..].to_vec();
        match db.rpush(key, elements, notifier) {
            Ok(list_len) => encode_integer(list_len as i64),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_lpop(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() < 2 || args.len() > 3 {
        encode_error("wrong number of arguments for 'lpop' command")
    } else if args.len() == 2 {
        match db.lpop(&args[1]) {
            Ok(Some(element)) => encode_bulk_string(&element),
            Ok(None) => encode_null_bulk_string(),
            Err(msg) => encode_error(msg),
        }
    } else {
        match args[2].parse::<usize>() {
            Ok(count) => match db.lpop_count(&args[1], count) {
                Ok(elements) => encode_array(&elements),
                Err(msg) => encode_error(msg),
            },
            Err(_) => encode_error("value is not an integer or out of range"),
        }
    }
}

pub fn handle_llen(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() != 2 {
        encode_error("wrong number of arguments for 'llen' command")
    } else {
        match db.llen(&args[1]) {
            Ok(list_len) => encode_integer(list_len as i64),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_lrange(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() != 4 {
        encode_error("wrong number of arguments for 'lrange' command")
    } else {
        let start_res = args[2].parse::<isize>();
        let stop_res = args[3].parse::<isize>();
        if start_res.is_err() || stop_res.is_err() {
            encode_error("value is not an integer or out of range")
        } else {
            match db.lrange(&args[1], start_res.unwrap(), stop_res.unwrap()) {
                Ok(elements) => encode_array(&elements),
                Err(msg) => encode_error(msg),
            }
        }
    }
}

pub fn handle_info(args: &[String], state: &ServerState) -> String {
    if args.len() > 1 && args[1].to_lowercase() == "replication" {
        let info = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            state.role, state.master_replid, state.master_repl_offset
        );
        encode_bulk_string(&info)
    } else {
        encode_bulk_string("")
    }
}

pub fn handle_incr(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() != 2 {
        encode_error("wrong number of arguments for 'incr' command")
    } else {
        match db.incr(&args[1]) {
            Ok(new_value) => encode_integer(new_value),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_replconf(_args: &[String]) -> String {
    encode_simple_string("OK")
}

pub fn handle_blpop(
    args: &[String],
    db_arc: &Arc<Mutex<InMemoryDB>>,
    notifier: &Arc<Mutex<Notifier>>,
) -> String {
    if args.len() != 3 {
        return encode_error("wrong number of arguments for 'blpop' command");
    }
    let key = args[1].clone();
    let timeout_res = args[2].parse::<f64>();

    if timeout_res.is_err() {
        return encode_error("timeout is not a float or out of range");
    }

    let timeout = Duration::from_secs_f64(timeout_res.unwrap());
    loop {
        let mut db_lock = db_arc.lock().unwrap();
        if let Ok(Some(element)) = db_lock.lpop(&key) {
            let items = vec![key, element];
            return encode_array(&items);
        }

        if timeout.as_secs_f64() == 0.0 {
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
            Err(_) => return encode_null_bulk_string(),
        }
    }
}

pub fn handle_psync(
    args: &[String],
    state: &ServerState,
    stream: &mut TcpStream,
    propagator: &mut CommandPropagator,
) -> std::io::Result<()> {
    if args.len() == 3 && args[1] == "?" && args[2] == "-1" {
        let response_str = format!("FULLRESYNC {} 0", state.master_replid);
        stream.write_all(encode_simple_string(&response_str).as_bytes())?;

        let rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2283a0400fa0c616f662d707265616d626c65c001ff25343234ff33313936";
        let rdb_content = hex::decode(rdb_hex).unwrap();
        let rdb_response = format!("${}\r\n", rdb_content.len());
        stream.write_all(rdb_response.as_bytes())?;
        stream.write_all(&rdb_content)?;

        let (tx, rx) = mpsc::channel::<String>();
        propagator.add_replica(tx);

        println!("Replica registered. Listening for propagated commands.");
        loop {
            match rx.recv() {
                Ok(command_str) => {
                    if stream.write_all(command_str.as_bytes()).is_err() {
                        eprintln!("Error propagating to replica. Disconnecting.");
                        break;
                    }
                }
                Err(_) => {
                    eprintln!("Propagator channel disconnected. Replica thread exiting.");
                    break;
                }
            }
        }
        Ok(())
    } else {
        stream.write_all(encode_error("PSYNC not supported").as_bytes())?;
        Ok(())
    }
}