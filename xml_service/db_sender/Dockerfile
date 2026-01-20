#Build
FROM rust:1.83-slim-bookworm AS builder 

WORKDIR /app
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release -p db_sender

#Run
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && \
    apt-get install -y libssl-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/db_sender /usr/local/bin/db_sender

CMD ["db_sender"]