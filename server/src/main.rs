use std::{net::TcpListener, thread, time::Duration, sync::{mpsc::{channel, Receiver}, Arc, RwLock}, env, process::Command};

use amiquip::{Connection, Exchange, Publish, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};

use crate::{utils::socket::{SocketReader, SocketWriter}, entities::{post::Post, comment::Comment}};
use serde_json::json;
mod utils;
mod entities;

const PORT_DEFAULT: &str = "12345";

const QUEUE_SCORES: &str = "QUEUE_SCORES";
const QUEUE_COMMENTS: &str = "QUEUE_COMMENTS";
const QUEUE_SCORE_AVG: &str = "QUEUE_SCORE_AVG";

const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

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

    println!("ifconfig: {:?}", Command::new("ifconfig").output());

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
    let queue_score_avg = channel.queue_declare(QUEUE_SCORE_AVG, QueueDeclareOptions::default()).unwrap();

    let listener;
    println!("binding on {}", format!("172.25.125.2:{}", port));
    match TcpListener::bind(format!("172.25.125.2:{}", port)) {
        Ok(tcp_listener) => {
            println!("server listening on port {}", port);
            listener = tcp_listener;
        }
        Err(err) => {
            panic!("could not start socket aceptor: {}", err)
        },
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

                let mut posts_done = false;
                let mut comments_done = false;
                loop {
                    if let Some(msg) = socket_reader.receive() {

                        let splited: Vec<&str>  = msg.split('|').collect();

                        let opcode = splited[0].parse::<u8>().unwrap();
                        let payload = splited[1];

                        match opcode {
                            OPCODE_POST_END => {
                                exchange.publish(Publish::new(
                                    "end_of_posts".as_bytes(),
                                    QUEUE_SCORES
                                )).unwrap();
                                posts_done = true;
                                println!("posts done");
                            }
                            OPCODE_COMMENT_END => {
                                comments_done = true;
                                println!("comments done");
                            }
                            OPCODE_POST => {
                                let post = Post::deserialize(payload.to_string());
                                exchange.publish(Publish::new(
                                    post.score.as_bytes(),
                                    QUEUE_SCORES
                                )).unwrap();
                            }
                            OPCODE_COMMENT => {
                                let comment = Comment::deserialize(payload.to_string());
                                exchange.publish(Publish::new(
                                    json!({
                                        "id": comment.id,
                                        "permalink": comment.permalink
                                    }).to_string().as_bytes(),
                                    QUEUE_COMMENTS
                                )).unwrap();
                            }
                            _ => {}
                        }

                        if posts_done && comments_done {
                            break;
                        }
                    }
                }
                
                // receive avg score from avg_worker
                let consumer_score_avg = queue_score_avg.consume(ConsumerOptions::default()).unwrap();
                for message in consumer_score_avg.receiver().iter() {
                    match message {
                        ConsumerMessage::Delivery(delivery) => {
                            let body = String::from_utf8_lossy(&delivery.body);
                            println!("avg result {}", body);
                            consumer_score_avg.ack(delivery).unwrap();
                            break;
                        }
                        _ => {}
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

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    if let Ok(_) = cleaner.join() {
        println!("cleaner stop")
    }

    println!("server shutdown");
}
