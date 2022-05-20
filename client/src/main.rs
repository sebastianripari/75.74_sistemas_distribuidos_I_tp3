use std::{net::{IpAddr, Ipv4Addr, TcpStream, SocketAddr}};

use crate::utils::socket::{SocketReader, SocketWriter};

const PORT: u16 = 12345;

mod utils;

fn main() {
    println!("client up");

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 25, 125, 2)), PORT);

    let mut reader;
    let mut writter;

    match TcpStream::connect(&address) {
        Ok(stream) => {
            println!("[alglobo] conectado con servicio hotel");
            let stream_clone = stream.try_clone().unwrap();
            reader = SocketReader::new(stream);
            writter = SocketWriter::new(stream_clone);
        }
        Err(err) => {
            println!("[alglobo] no se pudo conectar con el servicio hotel. Error: {:?}", err);
            panic!()
        }
    }

    writter.send("hi\n".to_string());
    let response = reader.receive();
    println!("response: {}", response.unwrap());
}
