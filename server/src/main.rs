use std::net::TcpListener;

use crate::{utils::socket::{SocketReader, SocketWriter}, entities::post::Post};

mod utils;
mod entities;

const PORT: u16 = 12345;

fn main() {
    println!("server start");

    let listener;
    match TcpListener::bind(format!("172.25.125.2:{}", PORT)) {
        Ok(tcp_listener) => {
            println!("server listening on port {}", PORT);
            listener = tcp_listener
        }
        Err(_) => panic!("could not start socket aceptor"),
    }
    if let Err(_) = listener.set_nonblocking(true) {
        panic!("could not set listener as non blocking")
    }

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                println!("new incoming connection");

                let stream_clone = stream.try_clone().unwrap();

                let (mut socket_reader, mut socket_writter) = (
                    SocketReader::new(stream),
                    SocketWriter::new(stream_clone)
                );

                loop {
                    if let Some(msg) = socket_reader.receive() {

                        if msg == "end_of_posts\n".to_string() {
                            break;
                        }

                        let post = Post::deserialize(msg);
                        println!("post received: {:?}", post);
                    }
                }
            }
            Err(_) => {
            }
        }
    }

    println!("server shutdown");
}
