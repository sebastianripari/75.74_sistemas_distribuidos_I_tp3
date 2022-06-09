use std::{
    io::{self, BufRead, Write},
    net::TcpStream,
};

pub struct SocketReader {
    pub reader: io::BufReader<TcpStream>,
}

impl SocketReader {
    pub fn new(stream: TcpStream) -> SocketReader {
        SocketReader {
            reader: io::BufReader::new(stream.try_clone().unwrap()),
        }
    }

    pub fn receive(&mut self) -> Option<String> {
        let mut mensaje = String::new();
        match self.reader.read_line(&mut mensaje) {
            Err(err) => {
                println!("{}", err);
                return None;
            }
            Ok(bytes) => {
                if bytes == 0 {
                    return None;
                } else {
                    mensaje.pop();
                    return Some(mensaje);
                }
            }
        }
    }
}

pub struct SocketWriter {
    pub writer: io::LineWriter<TcpStream>,
    pub socket: TcpStream,
}

impl SocketWriter {
    pub fn new(stream: TcpStream) -> SocketWriter {
        SocketWriter {
            writer: io::LineWriter::new(stream.try_clone().unwrap()),
            socket: stream,
        }
    }

    pub fn send_bytes(&mut self, msg: &[u8]) {
        self.socket.write_all(msg).unwrap();
    }

    pub fn send(&mut self, mensaje: String) {
        if let Err(err) = self.writer.write_all((mensaje + "\n").as_bytes()) {
            println!("{}", err);
        }
    }
}
