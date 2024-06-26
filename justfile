#!/usr/bin/env just --justfile

set dotenv-load

cargo-fix-all:
    cargo fix --allow-dirty --allow-staged --all
    cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features

cargo-build-all:
    cargo b --all-targets

run_jaeger:
    docker run --rm --name jaeger \
          -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
          -e COLLECTOR_OTLP_ENABLED=true \
          -p 6831:6831/udp \
          -p 6832:6832/udp \
          -p 5778:5778 \
          -p 16686:16686 \
          -p 4317:4317 \
          -p 4318:4318 \
          -p 14250:14250 \
          -p 14268:14268 \
          -p 14269:14269 \
          -p 9411:9411 \
          jaegertracing/all-in-one:1.35

echo:
    cargo build --bin echo
    ./maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10

unique_id:
    cargo build --release --bin unique_id
    ./maelstrom test -w unique-ids --bin ./target/release/unique_id --time-limit 30 --rate 10000 --concurrency 3n --node-count 5 --availability total --nemesis partition

broadcast-3a:
    cargo build --bin broadcast
    ./maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 5 --rate 20

broadcast-3b:
    cargo build --bin broadcast
    ./maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10

broadcast-3c:
    cargo build --bin broadcast --release
    ./maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 30 --rate 100 --nemesis partition

broadcast-3d:
    cargo build --bin broadcast --release
    ./maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

broadcast-3e:
    cargo build --bin broadcast --release
    ./maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

counter-4o:
    cargo build --bin counter
    ./maelstrom test -w g-counter --bin ./target/debug/counter --node-count 1 --time-limit 5 --rate 10

counter-4a:
    cargo build --bin counter --release
    ./maelstrom test -w g-counter --bin ./target/release/counter --node-count 3 --time-limit 20 --rate 100

counter-4b:
    cargo build --bin counter --release
    ./maelstrom test -w g-counter --bin ./target/release/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

counter-4x:
    cargo build --bin counter --release
    ./maelstrom test -w pn-counter --bin ./target/release/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition


kafka-5o:
     cargo build --bin kafka
     ./maelstrom test -w kafka --bin ./target/debug/kafka --node-count 1  --concurrency 2n --rate 10 --time-limit 10

kafka-5a:
     cargo build --bin kafka
     ./maelstrom test -w kafka --bin ./target/debug/kafka --node-count 1  --concurrency 2n --rate 1000 --time-limit 20

kafka-5b:
     cargo build --bin kafka
     ./maelstrom test -w kafka --bin ./target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

kafka-5c:
     cargo build --bin kafka  --release
     ./maelstrom test -w kafka --bin ./target/release/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

txn-rw-6o:
    cargo build --bin txn-rw
    ./maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw --node-count 1 --time-limit 10 --rate 10 --concurrency 2n --consistency-models read-uncommitted --availability total

txn-rw-6a:
    cargo build --bin txn-rw
    ./maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw --node-count 2 --time-limit 20 --rate 10 --concurrency 2n --consistency-models read-uncommitted --availability total

txn-rw-6b:
    cargo build --bin txn-rw
    ./maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw --node-count 2 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total --nemesis partition

txn-rw-6c:
    cargo build --bin txn-rw
    ./maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw --node-count 2 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-committed --availability total --nemesis partition

txn-rw-6d:
    cargo build --bin txn-rw
    ./maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw --node-count 2 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models monotonic-view --availability total --nemesis partition

gset-a1:
    cargo build --bin g_set
    ./maelstrom test -w g-set --bin ./target/debug/g_set --node-count 5 --time-limit 20 --rate 100

lin-kv-a2-0:
    cargo build --bin lin-kv
    ./maelstrom test -w lin-kv --bin ./target/debug/lin-kv --node-count 1 --time-limit 20 --rate 10 --concurrency 2n

lin-kv-a2-1:
    cargo build --bin lin-kv
    ./maelstrom test -w lin-kv --bin ./target/debug/lin-kv --node-count 5 --time-limit 20 --rate 10 --concurrency 2n --consistency-models linearizable


serve:
    ./maelstrom serve
