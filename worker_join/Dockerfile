FROM rust

RUN USER=root cargo new --bin tp2
WORKDIR /tp2

COPY worker_join/Cargo.lock ./Cargo.lock
COPY worker_join/Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY worker_join/src ./src
RUN cargo install --path .

ENTRYPOINT ["/bin/sh"]