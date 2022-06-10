use crate::client_responser::client_responser;
use crate::utils::socket::{SocketReader, SocketWriter};
use amiquip::{Exchange, Publish};
use constants::queues::QUEUE_INITIAL_STATE;
use std::sync::mpsc;
use std::{
    env,
    net::TcpListener,
    sync::{
        mpsc::{channel, Receiver},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};
use utils::{logger::logger_create, rabbitmq::rabbitmq_connect};

mod client_responser;
mod constants;
mod handlers;
mod messages;
mod utils;

const PORT_DEFAULT: &str = "12345";
const OPCODE_COMMENT_END: u8 = 3;

fn cleaner_handler(receiver_signal: Receiver<()>, running_lock: Arc<RwLock<bool>>) {
    receiver_signal.recv().unwrap();
    if let Ok(mut running) = running_lock.write() {
        *running = false;
    }
}

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let cleaner;
    let (sender_signal, receiver_signal) = channel();
    let sender_signal_clone = sender_signal.clone();
    let running_lock = Arc::new(RwLock::new(true));
    let mut running_lock_clone = running_lock.clone();
    ctrlc::set_handler(move || sender_signal.send(()).unwrap()).unwrap();
    cleaner = thread::spawn(move || cleaner_handler(receiver_signal, running_lock_clone));

    let mut port = PORT_DEFAULT.to_string();
    if let Ok(p) = env::var("SERVER_PORT") {
        port = p;
    }

    let mut n_consumers = 1;
    if let Ok(value) = env::var("N_CONSUMERS") {
        n_consumers = value.parse::<usize>().unwrap();
    }

    let (sender_clients, receiver_clients) = mpsc::channel();

    let logger_clone = logger.clone();

    // wait rabbitmq
    thread::sleep(Duration::from_secs(30));

    let client_handler = thread::spawn(move || client_responser(&logger_clone, receiver_clients));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
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
        }
    }

    if let Err(_) = listener.set_nonblocking(true) {
        panic!("could not set listener as non blocking")
    }

    let mut end = false;

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let stream_clone = stream.try_clone().unwrap();

                let (mut socket_reader, socket_writer) =
                    (SocketReader::new(stream), SocketWriter::new(stream_clone));

                sender_clients.send(socket_writer).unwrap();

                loop {
                    if let Some(msg) = socket_reader.receive() {
                        exchange
                            .publish(Publish::new(msg.as_bytes(), QUEUE_INITIAL_STATE))
                            .unwrap();

                        if msg == format!("{}|", OPCODE_COMMENT_END) {
                            end = true;
                        }

                        if end {
                            for _ in 0..n_consumers {
                                exchange
                                    .publish(Publish::new(
                                        format!("{}|", OPCODE_COMMENT_END).as_bytes(),
                                        QUEUE_INITIAL_STATE,
                                    ))
                                    .unwrap();
                            }
                            sender_signal_clone.send(()).unwrap();
                            break;
                        }
                    }
                }
                break;
            }
            Err(_) => {
                running_lock_clone = running_lock.clone();
                if let Ok(running) = running_lock_clone.read() {
                    if *running {
                        continue;
                    } else {
                        break;
                    }
                }
                thread::sleep(Duration::from_secs(5));
            }
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    if let Ok(_) = client_handler.join() {
        logger.info("client handler closed".to_string());
    }

    if let Ok(_) = cleaner.join() {
        logger.info("cleaner stop".to_string())
    }

    logger.info("server shutdown".to_string());
}
