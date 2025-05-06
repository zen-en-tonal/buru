# docker/Dockerfile
FROM rust:slim as builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y pkg-config libssl-dev

RUN cargo build --release --bin web

FROM debian:stable

WORKDIR /app

COPY --from=builder /app/target/release/web /app/web

ENV RUST_LOG=info
ENV CDN_BASE_URL=http://localhost/files

CMD ["./web"]
