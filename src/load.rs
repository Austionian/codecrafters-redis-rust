use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fs, io, path::PathBuf, string::FromUtf8Error};

fn io_ify(_x: FromUtf8Error) -> io::Error {
    io::ErrorKind::InvalidData.into()
}

enum LenEncoding {
    NextSixBits(u8),
    NextByte(u16),
    NextFourBytes(u32),
    _SpecialFormat,
}

fn get_string_encoded_len(buf: &[u8]) -> LenEncoding {
    let byte = buf.first().unwrap().clone();
    let encoding = byte & 0b1100_0000;

    match encoding {
        0x00 => LenEncoding::NextSixBits(byte),
        0x40 => {
            let first_byte = byte & 0b0011_1111;
            let second_byte = buf[1..2].first().clone().unwrap();
            let string_len = u16::from_ne_bytes([first_byte, *second_byte]);
            return LenEncoding::NextByte(string_len);
        }
        0x80 => {
            let string_len = u32::from_ne_bytes(buf[1..5].try_into().unwrap());
            return LenEncoding::NextFourBytes(string_len);
        }
        _ => unimplemented!("Special encodings not currently supported."),
    }
}

enum ExpiryType {
    Seconds,
    Milliseconds,
}

fn load_key_values(buf: &[u8], db: &mut HashMap<String, String>, save: bool) {
    if buf.first().unwrap() == &0xFF {
        return;
    }

    match buf.first() {
        Some(0xFD) => {
            println!("got FD");
            let (con, buf) = load_expiry(buf, ExpiryType::Seconds);
            load_key_values(buf, db, con);
        }
        Some(0xFC) => {
            println!("got FC");
            let (con, buf) = load_expiry(buf, ExpiryType::Milliseconds);
            load_key_values(buf, db, con);
        }
        Some(_) => {
            // Assumes only strings right now.
            println!("value type: {:?}", &buf[0..1]);

            match get_string_encoded_len(&buf[1..]) {
                LenEncoding::NextSixBits(len) => {
                    let key_start = 2;
                    let key = String::from_utf8(buf[key_start..key_start + len as usize].to_vec())
                        .unwrap();

                    // Assume the value type is 0
                    match get_string_encoded_len(&buf[key_start + len as usize..]) {
                        LenEncoding::NextSixBits(value_len) => {
                            let key_start = key_start + len as usize + 1;
                            let value = String::from_utf8(
                                buf[key_start as usize..key_start as usize + value_len as usize]
                                    .to_vec(),
                            )
                            .unwrap();

                            if save {
                                db.insert(key, value);
                            }

                            load_key_values(
                                &buf[key_start as usize + value_len as usize..],
                                db,
                                true,
                            );
                        }
                        _ => unimplemented!("Only string values!"),
                    }
                }
                _ => unimplemented!("Only small keys right now!"),
            }
        }
        None => panic!("Unexpected end of file."),
    }
}

fn load_expiry(buf: &[u8], expiry_type: ExpiryType) -> (bool, &[u8]) {
    match expiry_type {
        ExpiryType::Milliseconds => {
            let expiry_time = u64::from_ne_bytes(buf[1..9].try_into().unwrap());
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if expiry_time > current_time {
                return (true, &buf[9..]);
            }
            return (false, &buf[9..]);
        }
        ExpiryType::Seconds => {
            let expiry_time = u32::from_ne_bytes(buf[1..5].try_into().unwrap());
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
            if expiry_time > current_time {
                return (true, &buf[5..]);
            }
            return (false, &buf[5..]);
        }
    }
}

pub fn load_db(path: PathBuf, db: &mut HashMap<String, String>) -> Result<(), io::Error> {
    let file = fs::read(path)?;

    let redis = String::from_utf8(file[0..5].to_vec()).map_err(io_ify)?;

    if redis != "REDIS" {
        return Err(io::ErrorKind::InvalidData.into());
    }

    let version = String::from_utf8(file[5..9].to_vec()).map_err(io_ify)?;

    println!("Redis version: {version}");

    let mut expiry_time_place = 0;
    file.iter().enumerate().for_each(|(i, chunk)| {
        if *chunk == 0xFE {
            expiry_time_place = i + 1;
        }
    });

    println!("place: {:?}", expiry_time_place);

    let fb = expiry_time_place + 4;

    let value_type = fb as usize;

    let buf = &file[value_type..];

    load_key_values(buf, db, true);

    Ok(())
}
