FROM rust

COPY sender/Cargo.lock ./Cargo.lock
COPY sender/Cargo.toml ./Cargo.toml
COPY sender/src ./src

RUN cargo build --release

ENTRYPOINT ["/bin/sh"]
