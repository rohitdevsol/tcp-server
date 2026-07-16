use tcp_server::{helpers, read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_ten_messages() {
    let (addr, _) = helpers::start_server().await;
    let mut client_a = helpers::connect(addr).await;
    let mut client_b = helpers::connect(addr).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for i in 0..10u8 {
        let msg = format!("msg{i}");
        helpers::send_msg(&mut client_a, msg.as_bytes()).await;
        let response = helpers::recv_msg(&mut client_b).await;
        assert_eq!(response, msg.as_bytes());
    }
}

#[tokio::test]
async fn zero_length_payload() {
    let (addr, _) = helpers::start_server().await;
    let mut client_a = helpers::connect(addr).await;
    let mut client_b = helpers::connect(addr).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    helpers::send_msg(&mut client_a, b"").await;
    let response = helpers::recv_msg(&mut client_b).await;
    assert_eq!(response, b"");
}

#[tokio::test]
async fn drop_conn_mid_frame_does_not_break_server() {
    let (addr, _) = helpers::start_server().await;

    let mut client_a = helpers::connect(addr).await;
    let header = 4u32.to_be_bytes();
    helpers::send_raw(&mut client_a, &header).await;
    client_a.shutdown().await.unwrap();
    drop(client_a);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // now confirm the server is still alive and broadcasting correctly for others
    let mut client_b = helpers::connect(addr).await;
    let mut client_c = helpers::connect(addr).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    helpers::send_msg(&mut client_b, b"still alive").await;
    let response = helpers::recv_msg(&mut client_c).await;
    assert_eq!(response, b"still alive");
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

    let payload = b"or bhai kya hal hai";
    write_frame(&mut client_side, payload, 200).await.unwrap();
    let buf = read_frame(&mut server_side, 200).await.unwrap();

    assert_eq!(buf, payload);
}

#[tokio::test]
async fn read_frame_rejects_oversized_payload() {
    let (mut client_side, mut server_side) = tokio::io::duplex(1024);

    let payload = vec![b'y'; 300];
    write_frame(&mut client_side, &payload, 1000).await.unwrap();
    let result = read_frame(&mut server_side, 200).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn broadcast_delivers_to_other_client_not_sender() {
    let (addr, _) = helpers::start_server().await;
    let mut client_a = helpers::connect(addr).await;
    let mut client_b = helpers::connect(addr).await;

    helpers::send_msg(&mut client_a, b"hello").await;

    let received = helpers::recv_msg(&mut client_b).await;
    assert_eq!(received, b"hello");
}

#[tokio::test]
async fn sender_does_not_receive_own_broadcast() {
    let (addr, _) = helpers::start_server().await;
    let mut client_a = helpers::connect(addr).await;

    helpers::send_msg(&mut client_a, b"hello").await;

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        helpers::recv_msg(&mut client_a),
    )
    .await;

    assert!(result.is_err());
}
