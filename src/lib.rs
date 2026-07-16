use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn process_stream(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 10];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            println!("Client disconnected");
            return Ok(());
        }

        println!("Received {:?}", String::from_utf8_lossy(&buf[..n]));
        stream.write_all(&buf[..n]).await?;
    }
}
