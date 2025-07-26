use crate::types::RdbEntry;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn decode_length(buf: &[u8]) -> Result<(usize, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short".to_string());
    }
    let first = buf[0];
    let flag = first >> 6;
    match flag {
        0b00 => Ok(((first & 0x3F) as usize, 1)),
        0b01 => {
            if buf.len() < 2 {
                return Err("Buffer too short".to_string());
            }
            let size = (((first & 0x3F) as usize) << 8) | buf[1] as usize;
            Ok((size, 2))
        }
        0b10 => {
            if buf.len() < 5 {
                return Err("Buffer too short".to_string());
            }
            let size = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            Ok((size, 5))
        }
        _ => Err("Unsupported length encoding".to_string()),
    }
}

fn decode_string(buf: &[u8]) -> Result<(String, usize), String> {
    if buf.is_empty() {
        return Err("Buffer too short".to_string());
    }
    let first_byte = buf[0];
    let flag = first_byte >> 6;
    match flag {
        0b00 | 0b01 | 0b10 => {
            let (len, len_bytes_consumed) = decode_length(buf)?;
            let total_consumed = len_bytes_consumed + len;
            if buf.len() < total_consumed {
                return Err("Buffer too short".to_string());
            }
            let string_data = &buf[len_bytes_consumed..total_consumed];
            Ok((
                String::from_utf8_lossy(string_data).to_string(),
                total_consumed,
            ))
        }
        0b11 => match first_byte {
            0xC0 => Ok(((buf[1] as i8).to_string(), 2)),
            0xC1 => {
                if buf.len() < 3 {
                    return Err("Buffer too short".to_string());
                }
                Ok((i16::from_le_bytes([buf[1], buf[2]]).to_string(), 3))
            }
            0xC2 => {
                if buf.len() < 5 {
                    return Err("Buffer too short".to_string());
                }
                Ok((
                    i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]).to_string(),
                    5,
                ))
            }
            _ => Err(format!(
                "Unsupported special string format: {:02X}",
                first_byte
            )),
        },
        _ => unreachable!(),
    }
}

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

    let mut i = 9;
    let mut data = HashMap::new();
    let mut expiry_ms: Option<u64> = None;

    while i < buffer.len() {
        let byte = buffer[i];
        match byte {
            0xFA => {
                i += 1;
                let (_, key_skip) = decode_string(&buffer[i..])?;
                i += key_skip;
                let (_, val_skip) = decode_string(&buffer[i..])?;
                i += val_skip;
            }
            0xFE => {
                i += 1;
                let (_, db_num_skip) = decode_length(&buffer[i..])?;
                i += db_num_skip;
            }
            0xFB => {
                i += 1;
                let (_, ht_size_skip) = decode_length(&buffer[i..])?;
                i += ht_size_skip;
                let (_, expiry_ht_size_skip) = decode_length(&buffer[i..])?;
                i += expiry_ht_size_skip;
            }
            0xFD => {
                if i + 5 > buffer.len() {
                    return Err("Invalid RDB: file ends during FD expiry".to_string());
                }
                let ts_bytes: [u8; 4] = buffer[i + 1..i + 5]
                    .try_into()
                    .map_err(|e| format!("Error parsing FD expiry: {}", e))?;
                let timestamp_s = u32::from_le_bytes(ts_bytes);
                expiry_ms = Some(timestamp_s as u64 * 1000);
                i += 5;
            }
            0xFC => {
                if i + 9 > buffer.len() {
                    return Err("Invalid RDB: file ends during FC expiry".to_string());
                }
                let ts_bytes: [u8; 8] = buffer[i + 1..i + 9]
                    .try_into()
                    .map_err(|e| format!("Error parsing FC expiry: {}", e))?;
                expiry_ms = Some(u64::from_le_bytes(ts_bytes));
                i += 9;
            }
            0x00 => {
                i += 1;
                let (key, key_bytes_consumed) = decode_string(&buffer[i..])?;
                i += key_bytes_consumed;
                let (value, value_bytes_consumed) = decode_string(&buffer[i..])?;
                i += value_bytes_consumed;

                let entry = RdbEntry { value, expiry_ms };
                data.insert(key, entry);

                expiry_ms = None;
            }
            0xFF => break,
            _ => {
                return Err(format!("Unknown RDB section type: 0x{:02X}", byte));
            }
        }
    }
    Ok(data)
}