FROM rust
COPY worker_filter_students /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]