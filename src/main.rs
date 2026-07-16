use std::collections::HashMap;
use std::io::Error;
use tcp_server::{Clients, process_stream};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // we will make a map here
    let clients = Clients::new(Mutex::new(HashMap::new()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            println!("Got a new connection from client : {}", addr);
            let (read_half, write_half) = stream.into_split();
            clients.lock().await.insert(addr, write_half);

            let c = clients.clone();
            tokio::spawn(async move {
                if let Err(e) = process_stream(read_half, addr, c).await {
                    eprintln!("Connection error {e}");
                }
            });
        } else {
            println!("accept() failed")
        }
    }
}
