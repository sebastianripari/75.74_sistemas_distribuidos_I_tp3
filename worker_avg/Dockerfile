FROM rust
COPY worker_avg /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]