
   
use std::{net::TcpStream, io::{self, BufRead, Write}};

pub struct SocketReader {
    pub reader: io::BufReader<TcpStream>
}

impl SocketReader {
    pub fn new(stream: TcpStream) -> SocketReader {
        SocketReader {
            reader: io::BufReader::new(stream.try_clone().unwrap())
        }
    }

    pub fn receive(&mut self) -> Option<String> {
        let mut mensaje = String::new();
        match self.reader.read_line(&mut mensaje) {
            Err(err) => {
                println!("{}", err);
                return None
            }
            Ok(bytes) => {
                if bytes == 0 {
                    return None
                } else {
                    return Some(mensaje);
                }
            }
        }
    }
}

pub struct SocketWriter {
    pub writer: io::LineWriter<TcpStream>,
}

impl SocketWriter {
    pub fn new(stream: TcpStream) -> SocketWriter {
        SocketWriter {
            writer: io::LineWriter::new(stream)
        }
    }

    pub fn send(&mut self, mensaje: String) {
        if let Err(err) = self.writer.write(mensaje.as_bytes()) {
            println!("{}", err);
        }
        if let Err(err) = self.writer.flush() {
            println!("{}", err)
        };
    }
}