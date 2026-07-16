use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    loop {
        let (mut stream, socket_addr) = listener.accept().await.unwrap();
       tokio::spawn( async move {
            process_stream(&mut stream).await
        });

    }
}

// need a function that can handle stream
async fn process_stream(stream:&mut TcpStream) {
    // we need to process this stream
    let mut buf = [0;10];

    loop {
        let res = stream.read(&mut buf).await.unwrap();
        if res ==0 {
            break;
        } // this is here to deal with the FIN packet thingy i.e 0

        // now I know there are res number of bytes that are read
        println!("The client sent this message {:?}", String::from_utf8_lossy(&buf[..res]));
        stream.write_all(&buf[..res]).await.unwrap();
    }
}
