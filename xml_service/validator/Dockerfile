#Build 
FROM rust:1.88-slim-bookworm as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p validator

#Run 
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/validator /usr/local/bin/validator

CMD ["validator"]