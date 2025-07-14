use std::fs::File;
use std::io::Read;
use std::path::Path;

pub fn load_keys_from_rdb(path: &Path) -> Result<Vec<String>, String> {
    let mut file = File::open(path).map_err(|_| "RDB file not found")?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(|_| "Failed to read RDB")?;

    let mut keys = Vec::new();
    let mut i = 9; // skip header

    while i < buf.len() {
        let opcode = buf[i];
        i += 1;

        match opcode {
            0xFD => {
                if i >= buf.len() { break; }
                let key_len = buf[i] as usize;
                i += 1;
                if i + key_len > buf.len() { break; }

                let key = String::from_utf8(buf[i..i + key_len].to_vec()).map_err(|_| "Invalid UTF-8")?;
                keys.push(key);
                i += key_len;
            }
            0xFF => break,
            _ => continue,
        }
    }

    Ok(keys)
}
