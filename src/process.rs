use crate::Config;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};

enum Action {
    Echo,
    Ping,
    Set,
    SetExpiry,
    Get,
    Config,
    Keys,
}

fn get_action(s: &str, col: &Vec<&str>) -> Action {
    match s.to_lowercase().as_str() {
        "echo" => Action::Echo,
        "ping" => Action::Ping,
        "set" => {
            // If it's greater than 7 there will be additional
            // configurations to the set.
            if col.len() > 8 {
                return Action::SetExpiry;
            }
            return Action::Set;
        }
        "get" => Action::Get,
        "config" => Action::Config,
        "keys" => Action::Keys,
        _ => unimplemented!("Unknown action given."),
    }
}

const NULL: &[u8; 5] = b"$-1\r\n";
const PONG: &[u8; 7] = b"+PONG\r\n";
const OK: &[u8; 5] = b"+OK\r\n";

pub(crate) type DBLock = Arc<Mutex<DB>>;
pub(crate) type DB = HashMap<String, String>;

async fn send_ping(stream: &mut TcpStream) {
    let _ = stream.write_all(PONG).await;
}

async fn echo(stream: &mut TcpStream, message: Vec<&str>) {
    let _ = stream
        .write_all(format!("${}\r\n{}\r\n", message[4].len(), message[4]).as_bytes())
        .await;
}

async fn get(stream: &mut TcpStream, message: Vec<&str>, db: DBLock) {
    let val = db
        .lock()
        .unwrap()
        .get(message[4])
        .unwrap_or(&"".to_string())
        .clone();
    if val.is_empty() {
        let _ = stream.write_all(NULL).await;
    } else {
        let _ = stream
            .write_all(format!("${}\r\n{}\r\n", val.len(), val).as_bytes())
            .await;
    }
}

async fn set(stream: &mut TcpStream, message: Vec<&str>, db: DBLock) {
    let prev_val = db.lock().unwrap().insert(
        message[4].to_string().clone(),
        message[6].to_string().clone(),
    );

    if let Some(val) = prev_val {
        let _ = stream
            .write_all(format!("${}\r\n{}\r\n", val.len(), val).as_bytes())
            .await;
    } else {
        let _ = stream.write_all(OK).await;
    }
}

async fn set_expiry(stream: &mut TcpStream, message: Vec<&str>, db: DBLock) {
    let prev_val = db.lock().unwrap().insert(
        message[4].to_string().clone(),
        message[6].to_string().clone(),
    );

    if let Some(val) = prev_val {
        let _ = stream
            .write_all(format!("${}\r\n{}\r\n", val.len(), val).as_bytes())
            .await;
    } else {
        let _ = stream.write_all(b"+OK\r\n").await;
    }

    let db_lock = db.clone();
    let key = message[4].to_string();
    let duration = message[10].parse::<u64>().unwrap_or(200);

    tokio::spawn(async move {
        let _ = sleep(Duration::from_millis(duration)).await;
        let _lock = db_lock.lock().unwrap().remove(&key);
    });
}

async fn get_config(stream: &mut TcpStream, message: Vec<&str>, config: &Config) {
    match message[6] {
        "dir" | "DIR" => {
            let _ = stream
                .write_all(
                    format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        message[6].len(),
                        message[6],
                        config.dir.clone().unwrap().len(),
                        config.dir.clone().unwrap()
                    )
                    .as_bytes(),
                )
                .await;
        }
        "dbfilename" | "DBFILENAME" => {
            let _ = stream
                .write_all(
                    format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        message[6].len(),
                        message[6],
                        config.dbfilename.clone().unwrap().len(),
                        config.dbfilename.clone().unwrap()
                    )
                    .as_bytes(),
                )
                .await;
        }
        _ => unimplemented!("Not in config"),
    }
}

async fn get_keys(stream: &mut TcpStream, db: DBLock) {
    // Gets the first key out of the db
    let keys = db
        .lock()
        .unwrap()
        .keys()
        .map(|key| key.clone())
        .collect::<Vec<_>>();
    let key_string = keys.iter().fold("".to_string(), |acc, key| {
        acc + format!("${}\r\n{}\r\n", key.len(), key).as_str()
    });
    let _ = stream
        .write_all(format!("*{}\r\n{}", keys.len(), key_string).as_bytes())
        .await;
}

pub(crate) async fn process_socket(mut stream: TcpStream, db_lock: DBLock, config: &Config) {
    loop {
        let mut buf = [0; 128];
        let read_into = stream.read(&mut buf).await.unwrap_or(0);

        if read_into > 0 {
            let request = String::from_utf8(buf.to_vec()).unwrap_or(String::new());

            if let Some(arr_str) = request.strip_prefix('*') {
                let a = arr_str.split("\r\n").collect::<Vec<_>>();
                match get_action(a[2], &a) {
                    Action::Ping => send_ping(&mut stream).await,
                    Action::Get => {
                        let db_lock = db_lock.clone();
                        get(&mut stream, a, db_lock).await;
                    }
                    Action::Set => {
                        let db_lock = db_lock.clone();
                        set(&mut stream, a, db_lock).await;
                    }
                    Action::SetExpiry => {
                        let db_lock = db_lock.clone();
                        set_expiry(&mut stream, a, db_lock).await;
                    }
                    Action::Echo => echo(&mut stream, a).await,
                    Action::Config => get_config(&mut stream, a, &config).await,
                    Action::Keys => {
                        let db_lock = db_lock.clone();
                        get_keys(&mut stream, db_lock).await;
                    }
                }
            }
        };
    }
}
