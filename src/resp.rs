//use std::str::Lines;

pub fn parse_resp(input: &str) -> Vec<String> {
    let mut parts = vec![];
    let mut lines = input.lines();

    while let Some(line) = lines.next() {
        if line.starts_with('*') {
            continue; // array header
        } else if line.starts_with('$') {
            if let Some(data) = lines.next() {
                parts.push(data.to_string());
            }
        }
    }

    parts
}

pub fn encode_simple_string(s: &str) -> String {
    format!("+{}\r\n", s)
}

pub fn encode_bulk_string(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn encode_null_bulk_string() -> String {
    "$-1\r\n".to_string()
}

pub fn encode_array(items: &[String]) -> String {
    let mut result = format!("*{}\r\n", items.len());
    for item in items {
        result.push_str(&encode_bulk_string(item));
    }
    result
}

pub fn encode_error(msg: &str) -> String {
    format!("-ERR {}\r\n", msg)
}
