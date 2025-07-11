use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    let mut buf = [0; 512];

                    loop {
                        let read_count = match stream.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };

                        let input = String::from_utf8_lossy(&buf[..read_count]);
                        let parts = parse_resp(&input);

                        if parts.is_empty() {
                            let _ = stream.write_all(b"-ERR empty command\r\n");
                            continue;
                        }

                        match parts[0].to_uppercase().as_str() {
                            "PING" => {
                                let _ = stream.write_all(b"+PONG\r\n");
                            }
                            "ECHO" => {
                                if parts.len() < 2 {
                                    let _ = stream.write_all(b"-ERR wrong number of arguments for 'echo'\r\n");
                                } else {
                                    let msg = &parts[1];
                                    let resp = format!("${}\r\n{}\r\n", msg.len(), msg);
                                    let _ = stream.write_all(resp.as_bytes());
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

fn parse_resp(input: &str) -> Vec<String> {
    let mut lines = input.lines();
    let mut result = Vec::new();

    if let Some(first) = lines.next() {
        if !first.starts_with('*') {
            return vec![];
        }
    }

    while let Some(line) = lines.next() {
        if line.starts_with('$') {
            if let Some(val) = lines.next() {
                result.push(val.to_string());
            }
        }
    }

    result
}
