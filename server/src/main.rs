use crate::client_responser::client_responser;
use crate::utils::socket::{SocketReader, SocketWriter};
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
use utils::logger::logger_create;
use utils::middleware::{
    middleware_connect, middleware_consumer_end, middleware_create_channel,
    middleware_create_exchange, middleware_send_msg, middleware_send_msg_all_consumers,
};

mod client_responser;
mod constants;
mod handlers;
mod messages;
mod utils;

const PORT_DEFAULT: &str = "12345";
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT_END: u8 = 3;

fn cleaner_handler(receiver_signal: Receiver<()>, running_lock: Arc<RwLock<bool>>) {
    receiver_signal.recv().unwrap();
    if let Ok(mut running) = running_lock.write() {
        *running = false;
    }
}

fn main() {
    let logger = logger_create();
    let logger_clone = logger.clone();
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

    let (sender_clients, receiver_clients) = mpsc::channel();
    let client_handler = thread::spawn(move || client_responser(&logger_clone, receiver_clients));

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let exchange = middleware_create_exchange(&channel);

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

    let msg_posts_end: String = format!("{}|", OPCODE_POST_END);
    let msg_comments_end: String = format!("{}|", OPCODE_COMMENT_END);

    let mut n_end = 0;

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let stream_clone = stream.try_clone().unwrap();

                let (mut socket_reader, socket_writer) =
                    (SocketReader::new(stream), SocketWriter::new(stream_clone));

                sender_clients.send(socket_writer).unwrap();

                loop {
                    if let Some(msg) = socket_reader.receive() {

                        if msg == msg_posts_end {
                            middleware_send_msg_all_consumers(
                                &exchange,
                                &msg_posts_end,
                                [QUEUE_INITIAL_STATE].to_vec(),
                            );
                        }

                        if msg == msg_comments_end {
                            if  middleware_consumer_end(
                                &mut n_end,
                                &exchange,
                                [QUEUE_INITIAL_STATE].to_vec(),
                            ) {
                                sender_signal_clone.send(()).unwrap();
                                break;
                            }
                        }

                        middleware_send_msg(&exchange, &msg, QUEUE_INITIAL_STATE);
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

    connection.close().unwrap();

    if let Ok(_) = client_handler.join() {
        logger.info("client handler closed".to_string());
    }

    if let Ok(_) = cleaner.join() {
        logger.info("cleaner stop".to_string())
    }

    logger.info("server shutdown".to_string());
}
