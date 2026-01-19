#Build
FROM rust:1.88-slim-bookworm as builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release -p grpc-server

#Run
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/grpc-server /usr/local/bin/grpc-server

EXPOSE 50051

CMD ["grpc-server"]