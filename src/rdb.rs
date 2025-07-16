use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Decodes a "size-encoded" value from the RDB format.
/// Returns the decoded size and the number of bytes consumed to read it.
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

/// Decodes a string, which can be length-prefixed or a special integer format.
/// Returns the parsed String and the total number of bytes consumed from the buffer.
fn decode_string(buf: &[u8]) -> Result<(String, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short for string decoding".to_string());
    }
    let first_byte = buf[0];
    let flag = first_byte >> 6;
    match flag {
        0b00 | 0b01 | 0b10 => {
            // This is a standard length-prefixed string
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
            // This is a special integer format
            match first_byte {
                0xC0 => {
                    // 8-bit signed integer
                    if buf.len() < 2 {
                        return Err("Buffer too short for 8-bit int".to_string());
                    }
                    let val = buf[1] as i8;
                    Ok((val.to_string(), 2)) // Consumed flag byte + data byte
                }
                0xC1 => {
                    // 16-bit signed integer (little-endian)
                    if buf.len() < 3 {
                        return Err("Buffer too short for 16-bit int".to_string());
                    }
                    let val = i16::from_le_bytes([buf[1], buf[2]]);
                    Ok((val.to_string(), 3))
                }
                0xC2 => {
                    // 32-bit signed integer (little-endian)
                    if buf.len() < 5 {
                        return Err("Buffer too short for 32-bit int".to_string());
                    }
                    let val = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    Ok((val.to_string(), 5))
                }
                _ => Err(format!(
                    "Unsupported special string format: {:02X}",
                    first_byte
                )),
            }
        }
        _ => unreachable!(), // This case is not possible
    }
}

/// Load key-value pairs from an RDB file.
pub fn load_db_from_rdb(path: &Path) -> Result<HashMap<String, String>, String> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Ok(HashMap::new()), // If file doesn't exist, return empty DB
    };
    // **FIX 1:** The buffer must be a Vec<u8> to hold the file bytes.
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .map_err(|_| "Failed to read RDB file")?;

    if buffer.len() < 10 || &buffer[0..9] != b"REDIS0011" {
        return Err("Invalid RDB file header".to_string());
    }

    let mut i = 9; // Skip the header
    let mut data = HashMap::new();

    while i < buffer.len() {
        let byte = buffer[i];
        match byte {
            0xFA => {
                // Metadata section - skip key and value
                i += 1;
                let (_, key_skip) = decode_string(&buffer[i..])?;
                i += key_skip;
                let (_, val_skip) = decode_string(&buffer[i..])?;
                i += val_skip;
            }
            0xFE => {
                // Database selector
                i += 1; // Skip 0xFE
                let (_, db_num_skip) = decode_length(&buffer[i..])?;
                i += db_num_skip;
            }
            0xFB => {
                // Resize DB hints
                i += 1; // Skip 0xFB
                let (_, ht_size_skip) = decode_length(&buffer[i..])?;
                i += ht_size_skip;
                let (_, expiry_ht_size_skip) = decode_length(&buffer[i..])?;
                i += expiry_ht_size_skip;
            }
            0xFD => {
                // Expiry timestamp (seconds)
                i += 5; // 1 type byte + 4 timestamp bytes
            }
            0xFC => {
                // Expiry timestamp (milliseconds)
                i += 9; // 1 type byte + 8 timestamp bytes
            }
            0x00 => {
                // Value type is string, indicating a key-value pair
                i += 1; // Skip value type byte (0x00)

                let (key, key_bytes_consumed) = decode_string(&buffer[i..])?;
                i += key_bytes_consumed;

                // **FIX 2:** Get the value instead of discarding it with _.
                let (value, value_bytes_consumed) = decode_string(&buffer[i..])?;
                i += value_bytes_consumed;

                // **FIX 3:** Insert the key and value into the `data` HashMap.
                data.insert(key, value);
            }
            0xFF => break, // End of file
            _ => {
                return Err(format!(
                    "Unknown or unhandled RDB section type: 0x{:02X} at position {}",
                    byte, i
                ));
            }
        }
    }
    // **FIX 4:** Return the `data` HashMap, not a non-existent `keys` variable.
    Ok(data)
}