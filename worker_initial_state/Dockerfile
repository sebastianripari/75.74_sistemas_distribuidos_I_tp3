FROM rust
COPY worker_initial_state /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]