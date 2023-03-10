#!/bin/bash


tmux new -d -s store
tmux send-key  './target/release/proxy --address 127.0.0.1:12001 --backend-address 127.0.0.1:13001' ENTER
sleep 2
tmux split-window -h
tmux send-key  './target/release/store --address 127.0.0.1:11001 --proxy 127.0.0.1:13001' ENTER
#
# Don't change the seed node address below 
# For testing you could manually change it once the tmux session starts.
#
sleep 1
tmux select-pane -t 0
tmux split-window -v
tmux send-key  './target/release/store --address 127.0.0.1:11002 --connect 127.0.0.1:11001 --proxy 127.0.0.1:13001' ENTER
tmux split-window -v
tmux send-key  './target/release/store --address 127.0.0.1:11003 --connect 127.0.0.1:11001 --proxy 127.0.0.1:13001' ENTER
tmux select-pane -t 3
tmux split-window -v
tmux send-key  './target/release/store --address 127.0.0.1:11004 --connect 127.0.0.1:11001 --proxy 127.0.0.1:13001' ENTER
tmux split-window -v
tmux send-key  './target/release/store --address 127.0.0.1:11005 --connect 127.0.0.1:11001 --proxy 127.0.0.1:13001' ENTER
