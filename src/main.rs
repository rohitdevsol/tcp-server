use std::io::Error;
use tcp_server::{Broadcast, socket_reader_loop, socket_writer_loop};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // we will make a map here
    // let clients = Clients::new(Mutex::new(HashMap::new()));

    let (tx, _) = broadcast::channel::<Broadcast>(16);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            println!("Got a new connection from client : {}", addr);
            let (read_half, write_half) = stream.into_split();

            let rx = tx.subscribe();
            let tx_clone = tx.clone();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            tokio::spawn(async move {
                let _ = socket_reader_loop(read_half, tx_clone, addr).await;
                let _ = shutdown_tx.send(());
            });

            tokio::spawn(async move {
                if let Err(e) = socket_writer_loop(write_half, rx, addr, shutdown_rx).await {
                    eprintln!("Writer error {e}");
                }
            });
        } else {
            println!("accept() failed")
        }
    }
}
