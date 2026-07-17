use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;
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
pub use types::Broadcast;

//runs when someone sends a message
pub async fn socket_reader_loop(
    mut read_half: impl AsyncRead + Unpin,
    sender: broadcast::Sender<Broadcast>,
    socket_addr: SocketAddr,
) -> std::io::Result<()> {
    loop {
        let buf = read_frame(&mut read_half, MAX_BUFFER_SIZE).await?;
        let _ = sender.send((socket_addr, buf));
    }
}

// runs when someone receives some message
pub async fn socket_writer_loop(
    mut write_half: impl AsyncWrite + Unpin,
    mut receiver: broadcast::Receiver<Broadcast>,
    socket_addr: SocketAddr,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> std::io::Result<()> {
    loop {
        tokio::select! {
            msg = receiver.recv() => {
               match msg {
                     Ok(buf) => {
                        if buf.0 != socket_addr {
                            write_frame(&mut write_half, &buf.1, MAX_BUFFER_SIZE).await?;
                        }
                     }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("Lagged behind {n} messages, continuing");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(())

               }
            }
            _ = &mut shutdown_rx => {
                return Ok(());
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
