use std::net::SocketAddr;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use log::info;
use rand::seq::SliceRandom;
use tokio::time::{timeout, Duration};
use zeromq::{RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

use karyon::sleep;

#[derive(Parser, Debug)]
struct Args {
    /// Verbose
    #[arg(short, long)]
    verbose: bool,

    /// Listen address for the backend
    #[arg(long)]
    backend_address: SocketAddr,

    /// Listen address for the frontend
    #[arg(long)]
    address: SocketAddr,
}

async fn send_ping(backend: &mut RouterSocket, peers: &mut Vec<Bytes>) -> Result<()> {
    let mut rm_list = vec![];
    for peer in peers.iter() {
        let mut msg = ZmqMessage::from("PING");
        msg.push_front(vec![].into());
        msg.push_front(peer.clone());

        backend.send(msg).await?;
        if timeout(Duration::from_millis(150), backend.recv())
            .await
            .is_err()
        {
            rm_list.push(peer.clone());
        }
    }

    for peer in rm_list {
        let index = peers.iter().position(|p| p == &peer).unwrap();
        peers.remove(index);
    }
    Ok(())
}

fn choose_peer(peers: &mut Vec<Bytes>) -> Result<Bytes> {
    let peer = peers
        .choose(&mut rand::thread_rng())
        .ok_or(anyhow::anyhow!("Choose available backend server"))?
        .clone();

    let index = peers.iter().position(|p| p == &peer).unwrap();
    peers.remove(index);

    Ok(peer)
}

// This is a load balancing design pattern
// Another design is using the Router/Dealer with zeromq’s built-in proxy function:
// https://zguide.zeromq.org/docs/chapter2/#ZeroMQ-s-Built-In-Proxy-Function
async fn proxy(frontend: &mut RouterSocket, backend: &mut RouterSocket) -> Result<()> {
    let mut peers: Vec<Bytes> = vec![];
    loop {
        tokio::select! {
            msg = frontend.recv() => {
                if let Ok(mut msg) = msg {
                    if msg.len() != 3 {
                        continue
                    }

                    let peer = choose_peer(&mut peers);

                    // if there is no peer
                    if peer.is_err() {
                        continue
                    }

                    msg.push_front(vec![].into());
                    msg.push_front(peer?);
                    backend.send(msg).await?;
                }
            }
            msg = backend.recv() => {
                if let Ok(mut msg) = msg {
                    let client_addr = msg.get(2).unwrap();
                    peers.push(msg.get(0).unwrap().clone());
                    if client_addr != &Bytes::from("READY") {
                        frontend.send(msg.split_off(2)).await?;
                    }
                }
            }

            _ =  sleep(1) => {send_ping(backend, &mut peers).await?;},
        }
    }
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

    let backend_addr = args.backend_address;
    let frontend_addr = args.address;

    info!("Proxy Start!!");
    let mut backend = RouterSocket::new();
    let mut frontend = RouterSocket::new();

    backend.bind(&format!("tcp://{backend_addr}")).await?;
    frontend.bind(&format!("tcp://{frontend_addr}")).await?;

    info!("Backend addresss {backend_addr}");
    info!("Frontend addresss {frontend_addr}");

    proxy(&mut frontend, &mut backend).await?;
    Ok(())
}
