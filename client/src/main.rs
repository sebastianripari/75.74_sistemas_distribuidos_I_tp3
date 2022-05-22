use std::{net::{IpAddr, Ipv4Addr, TcpStream, SocketAddr}, time::Duration, thread, env};

use crate::utils::{socket::{SocketReader, SocketWriter}, file::{send_posts_from_file}};

const PORT_DEFAULT: u16 = 12345;

mod utils;
mod entities;

fn main() {
    println!("client up");

    let mut port = PORT_DEFAULT;
    if let Ok(p) = env::var("SERVER_PORT") {
        port = p.parse::<u16>().unwrap();
    }

    thread::sleep(Duration::from_secs(22));

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 25, 125, 2)), port);

    let mut reader;
    let mut writer;

    match TcpStream::connect(&address) {
        Ok(stream) => {
            println!("connected with the server");
            let stream_clone = stream.try_clone().unwrap();
            reader = SocketReader::new(stream);
            writer = SocketWriter::new(stream_clone);
        }
        Err(err) => {
            panic!("could not connect {}", err);
        }
    }

    send_posts_from_file("posts.csv".to_string(), &mut writer);
}
