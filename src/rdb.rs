use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::SystemTime;

// A new struct to hold both the value and an optional expiry timestamp.
pub struct RdbEntry {
    pub value: String,
    pub expiry_ms: Option<u64>, // Absolute Unix timestamp in milliseconds
}

// ... your `decode_length` and `decode_string` helper functions remain exactly the same ...
fn decode_length(buf: &[u8]) -> Result<(usize, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short for length decoding".to_string());
    }
    let first = buf[0];
    let flag = first >> 6;
    match flag {
        0b00 => Ok(((first & 0x3F) as usize, 1)), // 6-bit size
        0b01 => {
            if buf.len() < 2 {
                return Err("Buffer too short for 14-bit size".to_string());
            }
            let size = (((first & 0x3F) as usize) << 8) | buf[1] as usize;
            Ok((size, 2))
        }
        0b10 => {
            if buf.len() < 5 {
                return Err("Buffer too short for 32-bit size".to_string());
            }
            let size = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            Ok((size, 5))
        }
        0b11 => Err("Length-encoded string cannot start with 11".to_string()),
        _ => unreachable!(),
    }
}

fn decode_string(buf: &[u8]) -> Result<(String, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short for string decoding".to_string());
    }
    let first_byte = buf[0];
    let flag = first_byte >> 6;
    match flag {
        0b00 | 0b01 | 0b10 => {
            let (len, len_bytes_consumed) = decode_length(buf)?;
            let total_consumed = len_bytes_consumed + len;
            if buf.len() < total_consumed {
                return Err("Buffer too short for string data".to_string());
            }
            let string_data = &buf[len_bytes_consumed..total_consumed];
            let result_str = String::from_utf8(string_data.to_vec()).map_err(|e| e.to_string())?;
            Ok((result_str, total_consumed))
        }
        0b11 => {
            match first_byte {
                0xC0 => {
                    if buf.len() < 2 { return Err("Buffer too short for 8-bit int".to_string()); }
                    let val = buf[1] as i8;
                    Ok((val.to_string(), 2))
                }
                0xC1 => {
                    if buf.len() < 3 { return Err("Buffer too short for 16-bit int".to_string()); }
                    let val = i16::from_le_bytes([buf[1], buf[2]]);
                    Ok((val.to_string(), 3))
                }
                0xC2 => {
                    if buf.len() < 5 { return Err("Buffer too short for 32-bit int".to_string()); }
                    let val = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    Ok((val.to_string(), 5))
                }
                _ => Err(format!("Unsupported special string format: {:02X}", first_byte)),
            }
        }
        _ => unreachable!(),
    }
}


/// Load key-value pairs and their expiries from an RDB file.
pub fn load_db_from_rdb(path: &Path) -> Result<HashMap<String, RdbEntry>, String> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Ok(HashMap::new()),
    };
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .map_err(|_| "Failed to read RDB file")?;

    if buffer.len() < 10 || &buffer[0..9] != b"REDIS0011" {
        return Err("Invalid RDB file header".to_string());
    }

    let mut i = 9; // Skip the header
    let mut data = HashMap::new();
    // A temporary variable to hold the expiry for the *next* key.
    let mut expiry_ms: Option<u64> = None;

    while i < buffer.len() {
        let byte = buffer[i];
        match byte {
            0xFA => { /* ... unchanged ... */ }
            0xFE => { /* ... unchanged ... */ }
            0xFB => { /* ... unchanged ... */ }

            // **MODIFIED:** Now we read the timestamp instead of just skipping it.
            0xFD => { // Expiry timestamp in seconds (4-byte unsigned int, little-endian)
                let timestamp_s = u32::from_le_bytes(buffer[i+1..i+5].try_into().unwrap());
                expiry_ms = Some(timestamp_s as u64 * 1000); // Convert to milliseconds
                i += 5;
            }
            0xFC => { // Expiry timestamp in milliseconds (8-byte unsigned long, little-endian)
                let timestamp_ms = u64::from_le_bytes(buffer[i+1..i+9].try_into().unwrap());
                expiry_ms = Some(timestamp_ms);
                i += 9;
            }

            // **MODIFIED:** Now we use the expiry we just read.
            0x00 => {
                i += 1; // Skip value type byte

                let (key, key_bytes_consumed) = decode_string(&buffer[i..])?;
                i += key_bytes_consumed;
                let (value, value_bytes_consumed) = decode_string(&buffer[i..])?;
                i += value_bytes_consumed;

                // Create the entry, including the expiry if we found one.
                let entry = RdbEntry { value, expiry_ms };
                data.insert(key, entry);

                // Reset expiry for the next key.
                expiry_ms = None;
            }
            0xFF => break,
            _ => { /* ... unchanged ... */ }
        }
    }
    Ok(data)
}