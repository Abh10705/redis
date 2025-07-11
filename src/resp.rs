// src/resp.rs

/// Parses a RESP-formatted string and returns a Vec of command + args.
/// Example input: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
/// Output: ["ECHO", "hey"]
pub fn parse_resp(input: &str) -> Vec<String> {
    let mut lines = input.lines();
    let mut result = Vec::new();

    // First line should be *<count>
    if let Some(first_line) = lines.next() {
        if !first_line.starts_with('*') {
            return result; // Not a RESP array
        }
    }

    while let Some(line) = lines.next() {
        if line.starts_with('$') {
            // Next line should be the actual value
            if let Some(value) = lines.next() {
                result.push(value.to_string());
            }
        }
    }

    result
}
