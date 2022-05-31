FROM rust

RUN USER=root cargo new --bin tp2
WORKDIR tp2

COPY server/Cargo.lock ./Cargo.lock
COPY server/Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY server/src ./src
RUN cargo install --path .

ENTRYPOINT ["/bin/sh"]
