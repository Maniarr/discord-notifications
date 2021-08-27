FROM rust:1.54-slim as builder

RUN apt-get update

RUN apt-get install -y cmake librdkafka-dev g++

RUN mkdir -p /opt/builder

COPY . /opt/builder

WORKDIR /opt/builder

RUN cargo build --release

FROM debian:buster-slim

RUN mkdir -p /opt/webhook

COPY --from=builder /opt/builder/target/release/webhook /opt/webhook

CMD ["/opt/webhook/webhook"]
