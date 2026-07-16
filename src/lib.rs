use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod constants;
use constants::{MAX_BUFFER_SIZE, MAX_HEADER_SIZE};

pub async fn process_stream(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; MAX_BUFFER_SIZE];
    let mut header_buf = [0; MAX_HEADER_SIZE];
    loop {
        stream.read_exact(&mut header_buf).await?;

        let payload_size = u32::from_be_bytes(header_buf) as usize;

        if payload_size > MAX_BUFFER_SIZE {
            return Err(Error::new(ErrorKind::InvalidData, "payload too large"));
        }

        stream.read_exact(&mut buf[..payload_size]).await?;

        println!(
            "Received: {}",
            String::from_utf8_lossy(&buf[..payload_size])
        );

        stream.write_all(&header_buf).await?;
        stream.write_all(&buf[..payload_size]).await?;
    }
}
