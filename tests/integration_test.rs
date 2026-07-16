use tcp_server::{read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod helpers;

#[tokio::test]
async fn test_ten_messages() {
    let (addr, _) = helpers::start_server().await;
    let mut client = helpers::connect(addr).await;

    for i in 0..10u8 {
        let msg = format!("msg{i}");
        helpers::send_msg(&mut client, msg.as_bytes()).await;
        let response = helpers::recv_msg(&mut client).await;
        assert_eq!(response, msg.as_bytes());
    }
}

#[tokio::test]
async fn zero_length_payload() {
    let (addr, _) = helpers::start_server().await;
    let mut client = helpers::connect(addr).await;

    helpers::send_msg(&mut client, b"").await;
    let response = helpers::recv_msg(&mut client).await;
    assert_eq!(response, b"");
}

#[tokio::test]
async fn drop_conn_mid_frame() {
    let (addr, h) = helpers::start_server().await;
    let mut client = helpers::connect(addr).await;

    let header = 4u32.to_be_bytes();
    helpers::send_raw(&mut client, &header).await;

    client.shutdown().await.unwrap();
    let result = h.await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn over_limit_buffer() {
    let (addr, _) = helpers::start_server().await;
    let mut client = helpers::connect(addr).await;

    let msg = "y".repeat(257);
    helpers::send_msg(&mut client, msg.as_bytes()).await;

    let mut buf = vec![0; msg.len()];
    let res = client.read_exact(&mut buf).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn write_then_read_frame_over_duplex() {
    let (mut client_side, mut server_side) = tokio::io::duplex(1024);

    // write a frame into client_side

    let payload = b"or bhai kya hal hai";
    write_frame(&mut client_side, payload, 200).await.unwrap();

    // read it back out of server_side

    let buf = read_frame(&mut server_side, 200).await.unwrap();
    // assert the payload matches

    assert_eq!(buf, payload);
}
