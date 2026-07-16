use tcp_server::process_stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub async fn start_server() -> (std::net::SocketAddr, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        process_stream(stream).await
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
