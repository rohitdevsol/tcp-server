use std::net::SocketAddr;

// pub type Clients = Arc<Mutex<HashMap<SocketAddr, OwnedWriteHalf>>>;

pub type Broadcast = (SocketAddr, Vec<u8>);
