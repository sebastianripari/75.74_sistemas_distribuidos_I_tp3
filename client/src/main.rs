use std::{net::{IpAddr, Ipv4Addr, TcpStream, SocketAddr}};

use crate::utils::{socket::{SocketReader, SocketWriter}, file::{send_posts_from_file}};

const PORT: u16 = 12345;

mod utils;
mod entities;

fn main() {
    println!("client up");

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 25, 125, 2)), PORT);

    let mut reader;
    let mut writter;

    match TcpStream::connect(&address) {
        Ok(stream) => {
            println!("connected with the server");
            let stream_clone = stream.try_clone().unwrap();
            reader = SocketReader::new(stream);
            writter = SocketWriter::new(stream_clone);
        }
        Err(err) => {
            println!("could not connect {:?}", err);
            panic!()
        }
    }

    send_posts_from_file("posts_full.csv".to_string(), &mut writter);
}
