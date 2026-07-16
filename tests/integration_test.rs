use tcp_server::process_stream;
use tokio::io::AsyncWriteExt;
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
        client.write_all(msg.as_bytes()).await.unwrap();
    }
    client.shutdown().await.unwrap();
    h.await.unwrap()
}
