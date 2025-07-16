use std::fs::File;
use std::io::Read;
use std::path::Path;

// In rdb.rs

/// Decodes a string, which can be length-prefixed or a special integer format.
/// Returns the parsed String and the total number of bytes consumed from the buffer.
fn decode_string(buf: &[u8]) -> Result<(String, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short for string decoding".to_string());
    }

    let first_byte = buf[0];
    let flag = first_byte >> 6;

    match flag {
        0b00 | 0b01 | 0b10 => { // This is a standard length-prefixed string
            let (len, len_bytes_consumed) = decode_length(buf)?;
            let total_consumed = len_bytes_consumed + len;
            if buf.len() < total_consumed {
                return Err("Buffer too short for string data".to_string());
            }
            let string_data = &buf[len_bytes_consumed..total_consumed];
            let result_str = String::from_utf8(string_data.to_vec())
                .map_err(|e| e.to_string())?;
            Ok((result_str, total_consumed))
        }
        0b11 => { // This is a special integer format
            match first_byte {
                0xC0 => { // 8-bit signed integer
                    if buf.len() < 2 { return Err("Buffer too short for 8-bit int".to_string()); }
                    let val = buf[1] as i8;
                    Ok((val.to_string(), 2)) // Consumed flag byte + data byte
                }
                0xC1 => { // 16-bit signed integer (little-endian)
                    if buf.len() < 3 { return Err("Buffer too short for 16-bit int".to_string()); }
                    let val = i16::from_le_bytes([buf[1], buf[2]]);
                    Ok((val.to_string(), 3))
                }
                0xC2 => { // 32-bit signed integer (little-endian)
                    if buf.len() < 5 { return Err("Buffer too short for 32-bit int".to_string()); }
                    let val = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    Ok((val.to_string(), 5))
                }
                _ => Err(format!("Unsupported special string format: {:02X}", first_byte))
            }
        }
        _ => unreachable!(), // This case is not possible
    }
}
/// Load keys from an RDB file (only handles a single database and string keys)
pub fn load_keys_from_rdb(path: &Path) -> Result<Vec<String>, String> {
    let mut file = File::open(path).map_err(|_| "Could not open RDB file")?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|_| "Failed to read RDB file")?;

    let mut i = 9; // Skip the header (first 9 bytes): "REDIS0011"
    let mut keys = Vec::new();

    while i < buffer.len() {
        let byte = buffer[i];

        match byte {
            0xFA => {
                // Metadata section - skip it
                i += 1;
                let (name_len, skip) = decode_length(&buffer[i..])?;
                i += skip + name_len;
                let (val_len, skip) = decode_length(&buffer[i..])?;
                i += skip + val_len;
            }

             0xFB => { // Signals hash table size information
            i += 1; // Skip the 0xFB byte
            // Read and skip the hash table size
            let (_, skip) = decode_length(&buffer[i..])?;
            i += skip;
            // Read and skip the expiry hash table size
            let (_, skip) = decode_length(&buffer[i..])?;
            i += skip;
           }

            0xFE => {
                // Database selector - skip DB index
                i += 1;
                let (_, skip) = decode_length(&buffer[i..])?;
                i += skip;
            }

            0xFD | 0xFC => {
                // Expiry timestamp - skip 4 or 8 bytes
                i += if byte == 0xFD { 5 } else { 9 }; // +1 for type byte
            }

            0x00 => { // Value type is string, indicating a key-value pair
        i += 1; // Skip value type byte (0x00)

        // Decode the key using our new helper
        let (key, key_bytes_consumed) = decode_string(&buffer[i..])?;
        keys.push(key);
        i += key_bytes_consumed;

        // Decode the value just to figure out how many bytes to skip
        let (_value, value_bytes_consumed) = decode_string(&buffer[i..])?;
        i += value_bytes_consumed;
    }

            0xFF => break, // End of file
            _ => i += 1,   // Unknown section, just move on
        }
    }

    Ok(keys)
}

/// Decode a size-encoded value from buffer (used for strings, keys, etc.)
fn decode_length(buf: &[u8]) -> Result<(usize, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short".to_string());
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
        _ => Err("Unsupported or special string encoding".to_string()), // like int/lzf
    }
}
