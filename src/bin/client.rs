use tcp_server::{read_frame, write_frame};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let client = TcpStream::connect("127.0.0.1:3000").await.unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    tokio::spawn(async move {
        loop {
            match read_frame(&mut read_half, 256).await {
                Ok(msg) => println!("> {}", String::from_utf8_lossy(&msg)),
                Err(_) => break,
            }
        }
    });

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.unwrap() == 0 {
            break;
        }
        let msg = line.trim();
        if msg.is_empty() {
            continue;
        }
        write_frame(&mut write_half, msg.as_bytes(), 256)
            .await
            .unwrap();
    }
}
