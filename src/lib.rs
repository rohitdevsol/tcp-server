use std::io::{Error, ErrorKind};
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
use constants::{MAX_BUFFER_SIZE, MAX_HEADER_SIZE};

pub async fn process_stream(
    // not just tcp stream we gonna accept anything that is readable and writable
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
) -> std::io::Result<()> {
    loop {
        let buf = read_frame(&mut stream, MAX_BUFFER_SIZE).await?;

        println!("Received: {}", String::from_utf8_lossy(&buf));

        write_frame(&mut stream, &buf, MAX_BUFFER_SIZE).await?;
    }
}

pub async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    max_size: usize,
) -> std::io::Result<()> {
    if payload.len() > max_size {
        return Err(Error::new(ErrorKind::InvalidData, "payload too large"));
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
        return Err(Error::new(ErrorKind::InvalidData, "Payload too large"));
    }

    let mut buf = vec![0u8; payload_size];
    stream.read_exact(&mut buf).await?;

    Ok(buf)
}
