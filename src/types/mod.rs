use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, OwnedWriteHalf>>>;
