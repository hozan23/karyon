# Karyon

Simple distributed and in-memory key value store

## Usage

### Compile

    Cargo bulid --release

### Run the proxy

    ./target/release/proxy --address 127.0.0.1:12001 --backend-address 127.0.0.1:13001


### Run the stores  
For the nodes to connect and join the distributed network, we need a seed node
to initialize the network. This is only needed when the network starts. Once
there are two or more running nodes, the seed node could be shutdown without any
issue.

    ./target/release/store --address 127.0.0.1:11001 --proxy 127.0.0.1:13001

For any new node, it must have a seed node address(any running node address) 
at the start to join the network.

    ./target/release/store --address 127.0.0.1:11002 --connect 127.0.0.1:11001 --proxy 127.0.0.1:13001
    ./target/release/store --address 127.0.0.1:11003 --connect 127.0.0.1:11002 --proxy 127.0.0.1:13001

NOTE: You could use `run_script.sh` script to auto run the proxy and 5 stores 
inside tmux session.  

### Run the client

Put new item:

    ./target/release/client --proxy 127.0.0.1:12001 --put 1 "Hello"
    ./target/release/client --proxy 127.0.0.1:12001 --put 2 "World"

Get value:

    ./target/release/client --proxy 127.0.0.1:12001 --get 2

Delete item:

    ./target/release/client --proxy 127.0.0.1:12001 --delete 2




