FROM rust

RUN USER=root cargo new --bin tp2
WORKDIR /tp2

COPY worker_filter_students/Cargo.lock ./Cargo.lock
COPY worker_filter_students/Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY worker_filter_students/src ./src
RUN cargo install --path .

ENTRYPOINT ["/bin/sh"]