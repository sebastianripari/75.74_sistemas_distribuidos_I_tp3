FROM rust
COPY worker_map /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]