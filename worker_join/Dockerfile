FROM rust
COPY worker_join /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]