use tcp_server::process_stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn test_ten_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let h = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = process_stream(stream).await;
    });

    let mut client = TcpStream::connect(addr).await.unwrap();

    for i in 0..10u8 {
        let msg = format!("msg{i}");
        let header = msg.len();
        let mut payload = Vec::with_capacity(4 + msg.len());
        payload.extend_from_slice(&(header as u32).to_be_bytes());
        payload.extend_from_slice(msg.as_bytes());

        client.write_all(&payload).await.unwrap();

        let mut buf = vec![0; msg.len()];
        client.read_exact(&mut buf).await.unwrap();

        assert_eq!(buf, msg.as_bytes())
    }
}
