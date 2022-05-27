use std::{net::TcpListener, thread, time::Duration, sync::{mpsc::{channel, Receiver}, Arc, RwLock}, env};

use amiquip::{Connection, Exchange, Publish, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};

use crate::{utils::{socket::{SocketReader, SocketWriter}, logger::Logger}, entities::{comment::Comment}};
use serde_json::{json, Value};
mod utils;
mod entities;

const PORT_DEFAULT: &str = "12345";
const LOG_LEVEL: &str = "debug";

const QUEUE_INITIAL_STATE: &str = "QUEUE_INITIAL_STATE";
const QUEUE_TO_CLIENT: &str = "QUEUE_TO_CLIENT";

fn cleaner_handler(
    receiver_signal: Receiver<()>,
    running_lock: Arc<RwLock<bool>>
) {
    receiver_signal.recv().unwrap();
    if let Ok(mut running) = running_lock.write() {
        *running = false; 
    }
}

fn send_to_client(c: &mut Connection) {
    let channel = c.open_channel(None).unwrap();
    let queue_to_client = channel.queue_declare(QUEUE_TO_CLIENT, QueueDeclareOptions::default()).unwrap();
    let consumer_to_client = queue_to_client.consume(ConsumerOptions::default()).unwrap();

    // send to client responses
    for message in consumer_to_client.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let value: Value = serde_json::from_str(&body).unwrap();
                println!("send to client: {}", value)
            }
            _ => {}
        }
    }
}

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger.info("start".to_string());

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

    // wait rabbitmq
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
        Ok(connection) => {
            logger.info("connected with rabbitmq".to_string());
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);

    let listener;
    logger.info(format!("binding on 172.25.125.2:{}", port));
    match TcpListener::bind(format!("172.25.125.2:{}", port)) {
        Ok(tcp_listener) => {
            logger.info(format!("server listening on port {}", port));
            listener = tcp_listener;
        }
        Err(err) => {
            panic!("could not start socket aceptor: {}", err)
        },
    }
    
    if let Err(_) = listener.set_nonblocking(true) {
        panic!("could not set listener as non blocking")
    }

    thread::spawn(move || send_to_client(&mut rabbitmq_connection));

    for stream_result in listener.incoming() {

        match stream_result {
            Ok(stream) => {
                let stream_clone = stream.try_clone().unwrap();

                let (mut socket_reader, mut socket_writer) = (
                    SocketReader::new(stream),
                    SocketWriter::new(stream_clone)
                );                

                loop {
                    if let Some(msg) = socket_reader.receive() {
                        exchange.publish(Publish::new(
                            msg.as_bytes(),
                            QUEUE_INITIAL_STATE
                        )).unwrap();
                    }
                }
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

    /*
    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }
    */

    if let Ok(_) = cleaner.join() {
        logger.info("cleaner stop".to_string())
    }

    logger.info("server shutdown".to_string());
}
