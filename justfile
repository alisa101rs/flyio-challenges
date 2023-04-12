#!/usr/bin/env just --justfile


cargo-fix-all:
    cargo fix --allow-dirty --allow-staged --all
    cargo clippy --fix --allow-dirty --allow-staged --all

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
    cargo build --bin unique_id
    ./maelstrom test -w unique-ids --bin ./target/debug/unique_id --time-limit 30 --rate 2000 --node-count 5 --availability total --nemesis partition

broadcast-3a:
    cargo build --bin broadcast_async
    ./maelstrom test -w broadcast --bin ./target/debug/broadcast_async --node-count 1 --time-limit 5 --rate 20

broadcast-3b:
    cargo build --bin broadcast_async
    ./maelstrom test -w broadcast --bin ./target/debug/broadcast_async --node-count 5 --time-limit 20 --rate 10

broadcast-3c:
    cargo build --bin broadcast_async --release
    ./maelstrom test -w broadcast --bin ./target/release/broadcast_async --node-count 5 --time-limit 30 --rate 100 --nemesis partition

counter-4o:
    cargo build --bin counter
    ./maelstrom test -w g-counter --bin ./target/debug/counter --node-count 1 --time-limit 5 --rate 10

counter-4a:
    cargo build --bin counter --release
    ./maelstrom test -w g-counter --bin ./target/release/counter --node-count 3 --time-limit 20 --rate 100

counter-4b:
    cargo build --bin counter
    ./maelstrom test -w g-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

kafka-5o:
     cargo build --bin kafka
     ./maelstrom test -w kafka --bin ./target/debug/kafka --node-count 1  --concurrency 2n --rate 10 --time-limit 10

kafka-5a:
     cargo build --bin kafka
     ./maelstrom test -w kafka --bin ./target/debug/kafka --node-count 1  --concurrency 2n --rate 100 --time-limit 10



serve:
    ./maelstrom serve
