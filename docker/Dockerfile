# docker/Dockerfile
FROM rust:slim as builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y \
    clang \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libavfilter-dev \
    libavdevice-dev \
    pkg-config \
    libssl-dev

RUN cargo build --release --bin web


FROM debian:stable-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libavcodec59 \
    libavformat59 \
    libavutil57 \
    libavfilter8 \
    libavdevice59 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/web /app/web

ENV RUST_LOG=info
ENV CDN_BASE_URL=http://localhost/files

CMD ["./web"]
