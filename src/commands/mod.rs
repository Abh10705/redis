
use crate::db::InMemoryDB;
use crate::notifier::Notifier;
use crate::resp::*;
use crate::Config;
use std::sync::{Arc, Mutex};
use crate::ServerState; 
// Each function handles one specific command.

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
// In src/commands/mod.rs
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
        encode_error("wrong number of arguments for 'set' command")
    } else if args.len() >= 5 && args[3].to_uppercase() == "PX" {
        let key = args[1].clone();
        let value = args[2].clone();
        match args[4].parse::<u64>() {
            Ok(ms) => {
                db.set_with_expiry(key, value, ms);
                encode_simple_string("OK")
            }
            Err(_) => encode_error("value is not an integer or out of range"),
        }
    } else {
        let key = args[1].clone();
        let value = args[2].clone();
        db.set(key, value);
        encode_simple_string("OK")
    }
}

pub fn handle_config(args: &[String], config: &Arc<Config>) -> String {
    if args.len() < 3 || args[1].to_uppercase() != "GET" {
        encode_error("Only CONFIG GET is supported")
    } else {
        let key = &args[2];
        match key.as_str() {
            "dir" => encode_array(&["dir".to_string(), config.dir.clone()]),
            "dbfilename" => encode_array(&["dbfilename".to_string(), config.dbfilename.clone()]),
            _ => encode_error("Unknown CONFIG key"),
        }
    }
}

pub fn handle_keys(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() == 2 && args[1] == "*" {
        let keys = db.keys();
        encode_array(&keys)
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
}

pub fn handle_llen(args: &[String], db: &mut InMemoryDB) -> String {
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

pub fn handle_lrange(args: &[String], db: &mut InMemoryDB) -> String {
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
}

pub fn handle_incr(args: &[String], db: &mut InMemoryDB) -> String {
    if args.len() != 2 {
        encode_error("wrong number of arguments for 'incr' command")
    } else {
        let key = &args[1];
        match db.incr(key) {
            Ok(new_value) => encode_integer(new_value),
            Err(msg) => encode_error(msg),
        }
    }
}

pub fn handle_info(args: &[String], state: &ServerState) -> String {
    if args.len() > 1 && args[1].to_lowercase() == "replication" {
        // **MODIFIED:** Add the new fields to the response string.
        let info = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            state.role,
            state.master_replid,
            state.master_repl_offset
        );
        encode_bulk_string(&info)
    } else {
        encode_bulk_string("")
    }
}