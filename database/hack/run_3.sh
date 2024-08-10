#! /bin/bash
# Run 3 Null DB's with 3 different seeds

export RUST_LOG=info

#node 1
cargo run -- --roster=localhost:3002,localhost:3003 --id=3001 --port=8001 --encoding=json --dir=node1/ &
#node 2
cargo run -- --roster=localhost:3001,localhost:3003 --id=3002 --port=8002 --encoding=json --dir=node2/ &
#node 3
cargo run -- --roster=localhost:3001,localhost:3002 --id=3003 --port=8003 --encoding=json --dir=node3/ &

wait
