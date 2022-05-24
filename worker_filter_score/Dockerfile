FROM rust
COPY worker_filter_score /
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]