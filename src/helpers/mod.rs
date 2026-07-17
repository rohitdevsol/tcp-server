use crate::{Broadcast, socket_reader_loop, socket_writer_loop};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;

pub async fn start_server() -> (std::net::SocketAddr, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx, _) = broadcast::channel::<Broadcast>(16);

    let handle = tokio::spawn(async move {
        loop {
            if let Ok((stream, sock_addr)) = listener.accept().await {
                let (read_half, write_half) = stream.into_split();
                let rx = tx.subscribe();
                let tx_clone = tx.clone();

                tokio::spawn(async move {
                    if let Err(e) = socket_reader_loop(read_half, tx_clone, sock_addr).await {
                        eprintln!("Reader error {e}");
                    }
                });

                tokio::spawn(async move {
                    if let Err(e) = socket_writer_loop(write_half, rx, sock_addr).await {
                        eprintln!("Writer error {e}");
                    }
                });
            }
        }
    });

    (addr, handle)
}

pub async fn connect(addr: std::net::SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

pub async fn send_msg(stream: &mut TcpStream, payload: &[u8]) {
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(payload).await.unwrap();
}

pub async fn send_raw(stream: &mut TcpStream, bytes: &[u8]) {
    stream.write_all(bytes).await.unwrap();
}

pub async fn recv_msg(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.unwrap();
    buf
}
