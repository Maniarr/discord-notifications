FROM rust:1.58-slim-buster as builder

RUN apt-get update
RUN apt-get install -y libssl-dev build-essential zlib1g-dev pkg-config

COPY . /workspace

WORKDIR /workspace

RUN cargo build --release

FROM debian:buster-slim

RUN apt-get update && apt-get install -y ca-certificates

RUN update-ca-certificates --fresh

RUN mkdir -p /opt/discord

COPY --from=builder /workspace/target/release/discord /opt/discord

WORKDIR /opt/discord

CMD ["/opt/discord/discord"]
