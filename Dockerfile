# Builds the hl-stream binary into a small runtime image.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p hl-stream

FROM debian:bookworm-slim
COPY --from=build /src/target/release/hl-stream /usr/local/bin/hl-stream
# Peers connect back to 4001 and 4002; 8546 serves WebSocket clients and
# 9100 Prometheus metrics.
EXPOSE 4001 4002 8546 9100
VOLUME /data
ENTRYPOINT ["hl-stream"]
CMD ["--peers-cache", "/data/peers.json", "--metrics-addr", "0.0.0.0:9100", "serve", "--listen", "0.0.0.0:8546"]
