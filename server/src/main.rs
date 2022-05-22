use std::{net::TcpListener, thread, time::Duration, sync::{mpsc::{channel, Receiver}, Arc, RwLock}, env};

use amiquip::{Connection, Exchange, Publish, ConsumerOptions, QueueDeclareOptions};

use crate::{utils::socket::{SocketReader, SocketWriter}, entities::post::Post};

mod utils;
mod entities;

const PORT_DEFAULT: &str = "12345";

const QUEUE_SCORE: &str = "score";

fn cleaner_handler(
    receiver_signal: Receiver<()>,
    running_lock: Arc<RwLock<bool>>
) {
    receiver_signal.recv().unwrap();
    if let Ok(mut running) = running_lock.write() {
        *running = false; 
    }
}

fn main() {
    println!("server start");

    let mut port = PORT_DEFAULT.to_string();
    let cleaner;
    let (sender_signal, receiver_signal) = channel();
    let running_lock = Arc::new(RwLock::new(true));

    let mut running_lock_clone = running_lock.clone();

    if let Ok(p) = env::var("SERVER_PORT") {
        port = p;
    }

    ctrlc::set_handler(move || sender_signal.send(()).unwrap()).unwrap();
    cleaner = thread::spawn(move || cleaner_handler(receiver_signal, running_lock_clone));

    // wait rabbitmq start
    thread::sleep(Duration::from_secs(20));

    let mut rabbitmq_connection;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
        Ok(connection) => {
            println!("connected with rabbitmq");
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);
    let queue = channel.queue_declare(QUEUE_SCORE, QueueDeclareOptions::default()).unwrap();

    let listener;
    println!("binding on {}", format!("172.25.125.2:{}", port));
    match TcpListener::bind(format!("172.25.125.2:{}", port)) {
        Ok(tcp_listener) => {
            println!("server listening on port {}", port);
            listener = tcp_listener
        }
        Err(err) => panic!("could not start socket aceptor: {}", err),
    }
    if let Err(_) = listener.set_nonblocking(true) {
        panic!("could not set listener as non blocking")
    }

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let stream_clone = stream.try_clone().unwrap();

                let (mut socket_reader, mut socket_writer) = (
                    SocketReader::new(stream),
                    SocketWriter::new(stream_clone)
                );

                // send score to avg_worker
                loop {
                    if let Some(msg) = socket_reader.receive() {
                        if msg == "end_of_posts\n".to_string() {
                            exchange.publish(Publish::new(
                                "end_of_posts".as_bytes(),
                                "score"
                            )).unwrap();
                            break;
                        }
            
                        let post = Post::deserialize(msg);
                        println!("{} received", post.id);

                        exchange.publish(Publish::new(
                            post.score.as_bytes(),
                            "score"
                        )).unwrap();
                    }
                }
                
                // receive avg score from avg_worker
                // TO DO
            }
            Err(_) => {
                running_lock_clone = running_lock.clone();
                if let Ok(running) = running_lock_clone.read() {
                    if *running {
                        continue
                    } else {
                        break
                    }
                }
                thread::sleep(Duration::from_secs(5));
            }
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    if let Ok(_) = cleaner.join() {
        println!("cleaner stop")
    }

    println!("server shutdown");
}
