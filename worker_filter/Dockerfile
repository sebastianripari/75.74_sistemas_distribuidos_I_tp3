FROM rust
COPY worker_filter /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]