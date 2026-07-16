use std::io::Error;
use tcp_server::process_stream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            println!("Got a new connection from client : {}", addr);
            tokio::spawn(async move {
                if let Err(e) = process_stream(stream).await {
                    eprintln!("Connection error {e}");
                }
            });
        } else {
            println!("accept() failed")
        }
    }
}
