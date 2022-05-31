FROM rust

RUN USER=root cargo new --bin tp2
WORKDIR /tp2

COPY worker_initial_state/Cargo.lock ./Cargo.lock
COPY worker_initial_state/Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY worker_initial_state/src ./src
RUN cargo install --path .

ENTRYPOINT ["/bin/sh"]