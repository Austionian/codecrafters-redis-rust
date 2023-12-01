mod load;
mod process;

use clap::Parser;
use load::load_db;
use process::{process_socket, DB};
use std::path::PathBuf;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(short, long)]
    dir: Option<String>,

    #[arg(short, long)]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");

    let config = Config::parse();
    let config = Box::new(config);
    let config = Box::leak(config);

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let mut db: DB = HashMap::new();

    // Load the db into memory if config given.
    if let Some(dir) = &config.dir {
        let _ = load_db(
            PathBuf::from(format!("{}/{}", dir, config.dbfilename.clone().unwrap())),
            &mut db,
        );
    }

    let db_lock = Arc::new(Mutex::new(db));
    loop {
        let config = config as &Config;
        let db_l = db_lock.clone();
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let _ = process_socket(socket, db_l, config).await;
        });
    }
}
