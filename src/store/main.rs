use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use log::{error, info, trace, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Mutex,
    time::{timeout, Duration},
};
use zeromq::{RepSocket, ReqSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

use karyon::{sleep, sleep_millis, Item, RpcMsg, RpcMsgValueType};

#[derive(Parser, Debug)]
struct Args {
    /// Verbose
    #[arg(short, long)]
    verbose: bool,

    /// Listen address
    #[arg(short, long)]
    address: SocketAddr,

    /// Seed Address
    #[arg(short, long)]
    connect: Option<SocketAddr>,

    /// Proxy Address
    #[arg(short, long)]
    proxy: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LogEvent {
    Put(u64),
    Update(u64),
    Delete(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Logs {
    data: Vec<LogEvent>,
    clock: u64,
}

impl Logs {
    fn new() -> Self {
        Self {
            data: vec![],
            clock: 0,
        }
    }

    fn put(&mut self, key: u64) {
        self.clock += 1;
        self.data.push(LogEvent::Put(key))
    }

    fn update(&mut self, key: u64) {
        self.clock += 1;
        self.data.push(LogEvent::Update(key))
    }

    fn delete(&mut self, key: u64) {
        self.clock += 1;
        self.data.push(LogEvent::Delete(key))
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

// In-memory database
struct Store {
    database: HashMap<u64, String>,
    // List of events to maintain consistency between nodes.
    // NOTE In the real case, I would use Raft as a consensus algorithm.
    logs: Logs,
}

impl Store {
    fn new() -> Self {
        Self {
            database: HashMap::new(),
            logs: Logs::new(),
        }
    }

    fn put(&mut self, key: u64, value: &str) {
        if self.database.contains_key(&key) {
            // Add an Update event if the key already exists.
            self.logs.update(key);
        } else {
            self.logs.put(key);
        }
        self.database.insert(key, value.to_string());
    }

    fn get(&self, key: u64) -> Option<String> {
        self.database.get(&key).cloned()
    }

    fn delete(&mut self, key: u64) {
        self.logs.delete(key);
        self.database.remove(&key);
    }

    fn clock(&self) -> u64 {
        self.logs.clock
    }

    fn logs_from(&mut self, index: u64) -> Logs {
        let log_list = if self.logs.len() > index {
            self.logs.data[(index as usize)..].to_vec()
        } else {
            vec![]
        };
        Logs {
            data: log_list,
            clock: self.clock(),
        }
    }

    fn contains(&self, key: u64) -> bool {
        self.database.contains_key(&key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Operation {
    Get(u64),
    Put(Item),
    Delete(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Msg {
    // NOTE It will be a better design if we have a completely separate protocol and type for each
    // message group instead of mixing between network, sync, and db operations messages.

    // Network messages
    Ack,
    GetVersion,
    Version(f32),
    GetAddrs,
    Addrs(Vec<SocketAddr>),

    // Sync messages
    Clock(u64),
    GetLogs(u64),
    Logs(Logs),

    // DB operations messages
    Op(Operation),
}

#[async_trait]
trait Channel {
    type SocketType: Socket + SocketSend + SocketRecv;

    fn get_socket(&mut self) -> &mut Self::SocketType;

    async fn send(&mut self, msg: Msg) -> Result<()> {
        let msg: Vec<u8> = bincode::serialize(&msg)?;
        let socket = self.get_socket();
        tokio::select! {
            res = socket.send(msg.into()) => res.map_err(anyhow::Error::from),
            _ = sleep_millis(150) => Err(anyhow!("Timeout Error: sending a msg"))
        }
    }

    async fn recv(&mut self) -> Result<Msg> {
        let socket = self.get_socket();
        tokio::select! {
            msg = socket.recv() => {
                let msg: Vec<u8> = msg?.try_into().unwrap();
                let msg = bincode::deserialize::<Msg>(&msg)?;
                Ok(msg)
            }
            _ = sleep_millis(150) => Err(anyhow!("Timeout Error: waiting to recv a  msg"))
        }
    }

    async fn recv_wait(&mut self) -> Result<Msg> {
        let socket = self.get_socket();
        let msg: Vec<u8> = socket.recv().await?.try_into().unwrap();
        let msg = bincode::deserialize::<Msg>(&msg)?;
        Ok(msg)
    }
}

struct RepChannel {
    socket: RepSocket,
}

impl RepChannel {
    async fn listen(addr: SocketAddr) -> Result<Self> {
        let mut socket = RepSocket::new();
        let conn = socket.bind(&format!("tcp://{addr}")).await;
        if conn.is_err() {
            return Err(anyhow!("Failed to listen: {addr}"));
        }

        info!("Start listening to {addr}");
        Ok(Self { socket })
    }
}

struct ReqChannel {
    socket: ReqSocket,
}

impl ReqChannel {
    async fn connect(addr: SocketAddr) -> Result<Self> {
        let mut socket = ReqSocket::new();
        let conn = timeout(
            Duration::from_millis(600),
            socket.connect(&format!("tcp://{addr}")),
        )
        .await;

        if conn.is_err() {
            return Err(anyhow!("Failed to connect: {addr}"));
        }

        info!("Start connecting to {addr}");
        Ok(Self { socket })
    }
}

impl Channel for RepChannel {
    type SocketType = RepSocket;

    fn get_socket(&mut self) -> &mut Self::SocketType {
        &mut self.socket
    }
}

impl Channel for ReqChannel {
    type SocketType = ReqSocket;

    fn get_socket(&mut self) -> &mut Self::SocketType {
        &mut self.socket
    }
}

#[async_trait]
trait Protocol: Sync + Send {
    type ChannelType: Channel;

    async fn start(&self, channel: Arc<Mutex<Self::ChannelType>>, p2p: Arc<P2p>) -> Result<()>;
}

struct HandshakeProtocol {}

#[async_trait]
impl Protocol for HandshakeProtocol {
    type ChannelType = ReqChannel;
    async fn start(&self, channel: Arc<Mutex<Self::ChannelType>>, p2p: Arc<P2p>) -> Result<()> {
        info!("Start handshaking...");
        let mut channel = channel.lock().await;

        info!("Sending node version...");
        let v = Msg::GetVersion;
        channel.send(v).await?;
        let recv_version = channel.recv().await?;
        match recv_version {
            Msg::Version(rv_v) => {
                if rv_v != p2p.version {
                    error!("Not compatible version");
                    return Err(anyhow!("Not compatible version"));
                }
            }
            _ => unreachable!(),
        }

        info!("Handshaking complete!");
        Ok(())
    }
}

struct SyncReqProtocol {
    node: Arc<Node>,
    conn_notifier_sx: async_channel::Sender<SocketAddr>,
}

#[async_trait]
impl Protocol for SyncReqProtocol {
    type ChannelType = ReqChannel;
    async fn start(&self, channel: Arc<Mutex<Self::ChannelType>>, p2p: Arc<P2p>) -> Result<()> {
        info!("Start syncing...");

        loop {
            sleep(2).await;
            {
                let mut channel = channel.lock().await;
                trace!("Sending self address...");
                channel.send(Msg::Addrs(vec![p2p.address])).await?;
                channel.recv().await?;
            }

            {
                let mut channel = channel.lock().await;
                trace!("Sending GetAddrs msg...");
                channel.send(Msg::GetAddrs).await?;
                let addrs = channel.recv().await?;
                if let Msg::Addrs(addrs) = addrs {
                    for addr in addrs {
                        self.conn_notifier_sx.send(addr).await?;
                    }
                }
            }

            // This will keep consistency between nodes.
            // This is how it works: NodeA sends a Clock msg containing the length of its
            // logs to NodeB, NodeB responds back with its own clock. If NodeA’s is behind NodeB’s
            // Clock then it will send a GetLogs msg to NodeB asking for the missing logs.
            //
            // This is similar to the way Raft algorithm manages the logs.
            //
            let mut channel = channel.lock().await;
            let self_clock = self.node.store.lock().await.clock();
            channel.send(Msg::Clock(self_clock)).await?;
            trace!("Sending Clock msg...");
            if let Msg::Clock(remote_clock) = channel.recv().await? {
                if self_clock >= remote_clock {
                    continue;
                }

                channel.send(Msg::GetLogs(self_clock)).await?;
                let Msg::Logs(logs) = channel.recv().await? else { panic!() };

                let mut store = self.node.store.lock().await;
                for log in logs.data {
                    match log {
                        LogEvent::Put(key) | LogEvent::Update(key) => {
                            channel.send(Msg::Op(Operation::Get(key))).await?;
                            if let Msg::Op(Operation::Put(item)) = channel.recv().await? {
                                store.put(item.key, &item.value);
                            } else if let LogEvent::Update(_) = log {
                                store.logs.update(key);
                            } else {
                                store.logs.put(key);
                            }
                        }
                        LogEvent::Delete(key) => {
                            store.delete(key);
                        }
                    }
                }

                assert!(store.clock() == remote_clock)
            }
        }
    }
}

struct MsgHandlerProtocol {
    node: Arc<Node>,
    conn_notifier_sx: async_channel::Sender<SocketAddr>,
}

#[async_trait]
impl Protocol for MsgHandlerProtocol {
    type ChannelType = RepChannel;
    async fn start(&self, channel: Arc<Mutex<Self::ChannelType>>, p2p: Arc<P2p>) -> Result<()> {
        loop {
            let mut channel = channel.lock().await;
            let msg = channel.recv_wait().await?;
            match msg {
                Msg::GetVersion => {
                    trace!("Receive GetVersion msg");
                    let version = p2p.version;
                    channel.send(Msg::Version(version)).await?;
                }
                Msg::GetAddrs => {
                    trace!("Receive GetAddrs msg");
                    let address_pool = p2p.address_pool.lock().await;
                    let address_pool: Vec<SocketAddr> = address_pool.clone().into_iter().collect();
                    channel.send(Msg::Addrs(address_pool)).await?;
                }
                Msg::Addrs(addrs) => {
                    trace!("Receive Addrs msg");
                    for addr in addrs {
                        self.conn_notifier_sx.send(addr).await?;
                    }
                    channel.send(Msg::Ack).await?;
                }
                Msg::Clock(c) => {
                    trace!("Receive Clock msg {c}");
                    let c = self.node.store.lock().await.clock();
                    channel.send(Msg::Clock(c)).await?;
                }
                Msg::GetLogs(index) => {
                    trace!("Receive GetLogs msg {index}");
                    let mut store = self.node.store.lock().await;
                    channel.send(Msg::Logs(store.logs_from(index))).await?;
                }
                Msg::Op(op) => match op {
                    Operation::Put(item) => {
                        info!("Receive Put msg");
                        info!("Receive new item {}", item.key);
                        {
                            let mut store = self.node.store.lock().await;
                            store.put(item.key, &item.value);
                        }
                        channel.send(Msg::Ack).await?;
                    }
                    Operation::Get(key) => {
                        info!("Receive Get msg");
                        let store = self.node.store.lock().await;
                        if let Some(value) = store.get(key) {
                            channel
                                .send(Msg::Op(Operation::Put(Item { key, value })))
                                .await?;
                        } else {
                            channel.send(Msg::Ack).await?;
                        }
                    }
                    Operation::Delete(key) => {
                        info!("Receive Delete msg");
                        {
                            let mut store = self.node.store.lock().await;
                            store.delete(key);
                        }
                        channel.send(Msg::Ack).await?;
                    }
                },
                _ => unreachable!(),
            }
        }
    }
}

struct P2p {
    version: f32,
    address: SocketAddr,
    address_pool: Mutex<HashSet<SocketAddr>>,
    req_channels: Mutex<HashMap<SocketAddr, Arc<Mutex<ReqChannel>>>>,
    conn_notifier: (
        async_channel::Sender<SocketAddr>,
        async_channel::Receiver<SocketAddr>,
    ),
}

impl P2p {
    async fn new(version: f32, address: SocketAddr) -> Arc<Self> {
        let conn_notifier = async_channel::unbounded();
        let mut address_pool = HashSet::new();
        address_pool.insert(address);
        Arc::new(Self {
            version,
            address,
            address_pool: Mutex::new(address_pool),
            req_channels: Mutex::new(HashMap::new()),
            conn_notifier,
        })
    }

    fn get_new_conn_sx(&self) -> async_channel::Sender<SocketAddr> {
        self.conn_notifier.0.clone()
    }

    async fn broadcast(self: Arc<Self>, msg: Msg) -> Result<()> {
        let mut remove_list = vec![];
        let mut req_channels = self.req_channels.lock().await;

        for (addr, channel) in req_channels.iter_mut() {
            let mut channel = channel.lock().await;
            if let Err(e) = channel.send(msg.clone()).await {
                error!("Broadcasting Error: {e}");
                remove_list.push(addr);
            }
            if let Err(e) = channel.recv().await {
                error!("Broadcasting Error: {e}");
                remove_list.push(addr);
            }
        }

        for addr in remove_list.iter() {
            self.clone().remove_connection(addr).await;
        }
        Ok(())
    }

    async fn remove_connection(self: Arc<Self>, addr: &SocketAddr) {
        self.req_channels.lock().await.remove(addr);
        self.address_pool.lock().await.remove(addr);
        warn!("Remove {addr} from the address pool");
    }

    async fn new_connection(self: Arc<Self>, addr: SocketAddr) -> Result<Arc<Mutex<ReqChannel>>> {
        let req_channel = ReqChannel::connect(addr).await?;
        let req_channel = Arc::new(Mutex::new(req_channel));
        let protocol = HandshakeProtocol {};
        if protocol
            .start(req_channel.clone(), self.clone())
            .await
            .is_ok()
        {
            self.req_channels
                .lock()
                .await
                .insert(addr, req_channel.clone());
            self.address_pool.lock().await.insert(addr);
            info!("Added a new connection {addr}");
        }

        Ok(req_channel)
    }

    async fn run(
        self: Arc<Self>,
        msg_handler_protocol: Arc<dyn Protocol<ChannelType = RepChannel>>,
        sync_protocol: Arc<dyn Protocol<ChannelType = ReqChannel>>,
        peer_addr: Option<SocketAddr>,
    ) -> Result<()> {
        // Connect to the seed node
        if let Some(addr) = peer_addr {
            self.conn_notifier.0.send(addr).await?;
        }

        let self_cloned = self.clone();
        let conn_notifier_rv = self.conn_notifier.1.clone();
        tokio::spawn(async move {
            loop {
                // Wait to receive a new address,
                let new_addr = conn_notifier_rv.recv().await.unwrap();

                // Skip if the address is already in the address pool
                if self_cloned
                    .clone()
                    .address_pool
                    .lock()
                    .await
                    .contains(&new_addr)
                {
                    continue;
                }

                // Add the new address to the pool after successfully do the handshak.
                if let Ok(req_channel) = self_cloned.clone().new_connection(new_addr).await {
                    // Attach sync protocol
                    let sync_protocol_cloned = sync_protocol.clone();
                    let self_cloned = self_cloned.clone();
                    tokio::spawn(async move {
                        let sync_task = sync_protocol_cloned
                            .start(req_channel, self_cloned.clone())
                            .await;
                        if sync_task.is_err() {
                            self_cloned.remove_connection(&new_addr).await;
                        }
                    });
                }
            }
        });

        // Start listening
        let rep_channel = Arc::new(Mutex::new(RepChannel::listen(self.address).await?));

        // Attach msg handler protocol to the replay channel
        tokio::spawn(async move {
            msg_handler_protocol
                .start(rep_channel, self.clone())
                .await
                .unwrap();
        });

        Ok(())
    }
}

struct Node {
    id: String,
    store: Mutex<Store>,
}

impl Node {
    fn new() -> Arc<Self> {
        let mut rng = rand::thread_rng();
        Arc::new(Self {
            id: rng.gen::<u32>().to_string(),
            store: Mutex::new(Store::new()),
        })
    }

    // TODO Create a struct for RPC server
    async fn handle_rpc_msg(self: Arc<Self>, p2p: Arc<P2p>, rpc_msg: &RpcMsg) -> Result<RpcMsg> {
        match rpc_msg.value.clone() {
            RpcMsgValueType::Call(val) => match val.method.as_str() {
                "put" => {
                    info!("Receive RpcMsg::Put");

                    if val.params.len() != 2 {
                        return Ok(RpcMsg::new_error_reply(rpc_msg.id, "Wrong params."));
                    }

                    let key = val.params[0].parse::<u64>();
                    if key.is_err() {
                        return Ok(RpcMsg::new_error_reply(rpc_msg.id, "Wrong params."));
                    }
                    let value = val.params[1].clone();
                    let item = Item { key: key?, value };

                    info!("Broadcast item {:?}", item);
                    let msg = Msg::Op(Operation::Put(item.clone()));
                    p2p.clone().broadcast(msg).await?;
                    self.store.lock().await.put(item.key, &item.value);

                    Ok(RpcMsg::new_reply(rpc_msg.id, true))
                }
                "get" => {
                    info!("Receive RpcMsg::Get");
                    let key = val.params[0].parse::<u64>();
                    if key.is_err() {
                        return Ok(RpcMsg::new_error_reply(rpc_msg.id, "Wrong params."));
                    }

                    let value = self.store.lock().await.get(key?);
                    match value {
                        Some(v) => Ok(RpcMsg::new_reply(rpc_msg.id, v)),
                        None => Ok(RpcMsg::new_error_reply(rpc_msg.id, "Item not found.")),
                    }
                }
                "delete" => {
                    info!("Receive RpcMsg::Delete");
                    let key = val.params[0].parse::<u64>();
                    if key.is_err() {
                        return Ok(RpcMsg::new_error_reply(rpc_msg.id, "Wrong params."));
                    }
                    let key = key?;

                    let mut store = self.store.lock().await;
                    if store.contains(key) {
                        info!("Broadcast Delete op item {:?}", key);
                        let msg = Msg::Op(Operation::Delete(key));
                        p2p.clone().broadcast(msg).await?;
                        store.delete(key);
                        Ok(RpcMsg::new_reply(rpc_msg.id, true))
                    } else {
                        Ok(RpcMsg::new_error_reply(rpc_msg.id, "Item not found."))
                    }
                }

                _ => Ok(RpcMsg::new_error_reply(rpc_msg.id, "Unsupported method.")),
            },
            RpcMsgValueType::Reply(_) => todo!(),
        }
    }

    async fn start_proxy_conn(
        self: Arc<Self>,
        p2p: Arc<P2p>,
        proxy_addr: SocketAddr,
        stop_signal: async_channel::Receiver<()>,
    ) -> Result<()> {
        info!("Connect to the proxy {proxy_addr}");
        let mut socket = ReqSocket::new();
        tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            socket.connect(&format!("tcp://{proxy_addr}")),
        )
        .await
        .expect("Connect to the proxy server")?;

        socket.send("READY".into()).await?;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = socket.recv() => {
                        let result: Result<(RpcMsg, Vec<u8>)> = {
                            let msg = msg?;
                            let client_addr = msg.get(0).unwrap();

                            if client_addr == &Bytes::from("PING") {
                                socket.send("PONG".into()).await?;
                                continue
                            }

                            let rpc_msg = msg.get(2).unwrap();
                            let rpc_msg = bincode::deserialize::<RpcMsg>(rpc_msg)?;
                            let msg_reply = self.clone().handle_rpc_msg(p2p.clone(), &rpc_msg).await?;
                            Ok((msg_reply, client_addr.to_vec()))
                        };

                        if result.is_ok() {
                            let (msg_reply, client_addr) = result?;
                            let msg_reply_ser = bincode::serialize(&msg_reply)?;
                            let mut msg: ZmqMessage = msg_reply_ser.into();
                            msg.push_front(vec![].into());
                            msg.push_front(client_addr.into());
                            socket.send(msg).await?;
                        }

                    }
                    _ = stop_signal.recv() => {
                        socket.close().await;
                        break
                    }
                }
            }
            Ok(()) as Result<()>
        });
        Ok(())
    }

    async fn start(
        self: Arc<Self>,
        p2p: Arc<P2p>,
        proxy_addr: SocketAddr,
        stop_signal: async_channel::Receiver<()>,
    ) -> Result<()> {
        info!("Start Node {}", self.id);

        // Wait until a connection is established with the proxy.
        self.clone()
            .start_proxy_conn(p2p.clone(), proxy_addr, stop_signal.clone())
            .await?;

        loop {
            tokio::select! {
                _ = sleep(3) => {
                    // XXX this is for demonstration purposes.
                    info!("data {:?}", self.store.lock().await.database);
                    info!("logs {:?}", self.store.lock().await.logs.data);
                    info!("clock {:?}", self.store.lock().await.clock());
                }
                _ = stop_signal.recv() => return Ok(()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let version = 0.1;

    let log_level = if args.verbose {
        simplelog::LevelFilter::Trace
    } else {
        simplelog::LevelFilter::Info
    };
    simplelog::SimpleLogger::init(log_level, simplelog::Config::default())?;

    let node = Node::new();

    let p2p = P2p::new(version, args.address).await;
    let conn_notifier_sx = p2p.get_new_conn_sx();

    let msg_handler_protocol = Arc::new(MsgHandlerProtocol {
        node: node.clone(),
        conn_notifier_sx: conn_notifier_sx.clone(),
    });

    let sync_protocol = Arc::new(SyncReqProtocol {
        node: node.clone(),
        conn_notifier_sx,
    });

    p2p.clone()
        .run(msg_handler_protocol, sync_protocol, args.connect)
        .await?;

    // Wait for CTRL+C signal then gracefully shutdown
    let (stop_signal_sx, stop_signal) = async_channel::unbounded();
    ctrlc_async::set_async_handler(async move {
        stop_signal_sx.send(()).await.unwrap();
    })?;

    node.start(p2p, args.proxy, stop_signal.clone()).await
}
