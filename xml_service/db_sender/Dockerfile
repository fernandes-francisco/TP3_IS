#BUILD
FROM rust:1.88-slim-bookworm as builder

WORKDIR /app

COPY . .

RUN cargo build --release -p converter
#RUNTIME
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/converter /usr/local/bin/converter

CMD ["converter"]