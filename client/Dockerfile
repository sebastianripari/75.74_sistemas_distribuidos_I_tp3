FROM rust

RUN USER=root cargo new --bin tp2
WORKDIR tp2

COPY client/Cargo.lock ./Cargo.lock
COPY client/Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY client/src ./src
RUN cargo install --path .

ENTRYPOINT ["/bin/sh"]
