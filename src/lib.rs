use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// AsyncReadExt is a blanket implementation on top of AsyncRead and same goes for the AsyncWriteExt

/**
=== A NOTE ON UNPIN ===
- async functions build a paused-and-resumable state machine under the hood.
- and some of those machines are not safe to move around in memory once they have started
- Unpin label says - this is safe to move

 This was given by AI .
*/
mod constants;
pub mod helpers;
mod types;

use constants::{MAX_BUFFER_SIZE, MAX_HEADER_SIZE};
pub use types::Clients;

pub async fn process_stream(
    // not just tcp stream we gonna accept anything that is readable and writable
    mut read_stream: impl AsyncRead + Unpin,
    curr_socket_addr: SocketAddr,
    clients: Clients,
) -> std::io::Result<()> {
    let result = process_loop(&mut read_stream, curr_socket_addr, &clients).await;
    clients.lock().await.remove(&curr_socket_addr);
    result
}

async fn process_loop(
    read_stream: &mut (impl AsyncRead + Unpin),
    curr_socket_addr: SocketAddr,
    clients: &Clients,
) -> std::io::Result<()> {
    loop {
        let buf = read_frame(read_stream, MAX_BUFFER_SIZE).await?;
        println!("Received: {}", String::from_utf8_lossy(&buf));

        let mut all_clients = clients.lock().await;
        for (sock_addr, write_stream) in all_clients.iter_mut() {
            if *sock_addr != curr_socket_addr {
                match write_frame(write_stream, &buf, MAX_BUFFER_SIZE).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error while writing :{}", e);
                    }
                }
            }
        }
    }
}
pub async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    max_size: usize,
) -> std::io::Result<()> {
    if payload.len() > max_size {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "[Write Frame] payload too large",
        ));
    }
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

pub async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    max_size: usize,
) -> std::io::Result<Vec<u8>> {
    let mut header_buf = [0; MAX_HEADER_SIZE];
    stream.read_exact(&mut header_buf).await?;

    let payload_size = u32::from_be_bytes(header_buf) as usize;
    eprintln!("Read payload size is: {}", payload_size);

    if payload_size > max_size {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "[Read Frame] Payload too large",
        ));
    }

    let mut buf = vec![0u8; payload_size];
    stream.read_exact(&mut buf).await?;

    Ok(buf)
}
