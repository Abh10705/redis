    use std::fs::File;
use std::io::Read;
use std::path::Path;

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

            0x00 => {
                i += 1; // Skip value type byte (0x00 = string)

                // Decode key
                let (key_len, key_skip) = decode_length(&buffer[i..])?;
                i += key_skip;
                let key_bytes = &buffer[i..i + key_len];
                let key = String::from_utf8_lossy(key_bytes).to_string();
                keys.push(key);
                i += key_len;

                // Decode value (but we ignore it)
                let (val_len, val_skip) = decode_length(&buffer[i..])?;
                i += val_skip + val_len;
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
