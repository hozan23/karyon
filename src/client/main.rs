use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use log::{error, info};
use rand::Rng;
use serde::de::DeserializeOwned;
use tokio::time::{timeout, Duration};
use zeromq::{ReqSocket, Socket, SocketRecv, SocketSend};

use karyon::{RpcMsg, RpcMsgCallValue, RpcMsgReplyValue, RpcMsgValueType};

#[derive(Parser, Debug)]
struct Args {
    /// Verbose
    #[arg(short, long)]
    verbose: bool,

    /// Proxy address
    #[arg(long, default_value = "127.0.0.1:12001")]
    proxy: SocketAddr,

    /// Put item
    #[arg(long, num_args(2))]
    put: Vec<String>,

    /// Get item value
    #[arg(long)]
    get: Option<u64>,

    /// Delete item
    #[arg(long)]
    delete: Option<u64>,
}

async fn send_rpc_request<T>(
    socket: &mut ReqSocket,
    method: &str,
    params: Vec<String>,
) -> Result<()>
where
    T: std::fmt::Display + DeserializeOwned,
{
    let mut rng = rand::thread_rng();
    let id = rng.gen::<u64>();
    let rpc_msg = RpcMsg::new_call(
        id,
        RpcMsgCallValue {
            method: method.to_string(),
            params,
        },
    );

    let rpc_msg_ser = bincode::serialize(&rpc_msg)?;
    socket.send(rpc_msg_ser.into()).await?;

    let msg_recv = timeout(Duration::from_secs(1), socket.recv()).await;

    if msg_recv.is_err() {
        error!("Timeout Error");
        return Ok(());
    }

    let msg: Vec<u8> = msg_recv??.try_into().unwrap();

    let rpc_msg_reply = bincode::deserialize::<RpcMsg>(&msg);
    if rpc_msg_reply.is_err() {
        error!("Error: Receive unknown message");
        return Ok(());
    }

    if let RpcMsgValueType::Reply(rep) = rpc_msg_reply?.value {
        match rep {
            RpcMsgReplyValue::Success(v) => {
                let msg = bincode::deserialize::<T>(&v)?;
                info!("{method}: {msg}");
            }
            RpcMsgReplyValue::Error(e) => {
                error!("{method} Error: {e}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = if args.verbose {
        simplelog::LevelFilter::Trace
    } else {
        simplelog::LevelFilter::Info
    };
    simplelog::SimpleLogger::init(log_level, simplelog::Config::default()).unwrap();

    let addr = args.proxy;
    let mut socket = ReqSocket::new();
    socket.connect(&format!("tcp://{addr}")).await?;

    if !args.put.is_empty() {
        send_rpc_request::<bool>(&mut socket, "put", args.put).await?;
    } else if let Some(key) = args.get {
        send_rpc_request::<String>(&mut socket, "get", vec![key.to_string()]).await?;
    } else if let Some(key) = args.delete {
        send_rpc_request::<bool>(&mut socket, "delete", vec![key.to_string()]).await?;
    }

    socket.close().await;
    Ok(())
}
