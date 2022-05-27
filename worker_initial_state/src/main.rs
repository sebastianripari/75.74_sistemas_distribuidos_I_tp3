use std::{thread, time::Duration, env};

use amiquip::{ConsumerMessage, Connection, Exchange, ConsumerOptions, QueueDeclareOptions, Publish};
use serde_json::json;

use crate::{utils::logger::{Logger}, entities::post::Post};

mod utils;
mod entities;

// queue input
const QUEUE_INITIAL_STATE: &str = "QUEUE_INITIAL_STATE";

// queue output
const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";
const QUEUE_POSTS_TO_FILTER_SCORE: &str = "QUEUE_POSTS_TO_FILTER_SCORE";
const QUEUE_COMMENTS_TO_FILTER_STUDENTS: &str = "QUEUE_COMMENTS_TO_FILTER_STUDENTS";

const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

const LOG_LEVEL: &str = "debug";

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);
    
    println!("Hello, world!");

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

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
    let queue = channel.queue_declare(QUEUE_INITIAL_STATE, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    loop {
        let mut n_post_received = 0;
        let mut posts_done = false;
        let mut comments_done = false;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
    
                    let splited: Vec<&str>  = body.split('|').collect();
    
                    let opcode = splited[0].parse::<u8>().unwrap();
                    let payload = splited[1..].join("|");
    
                    match opcode {
                        OPCODE_POST_END => {
                            posts_done = true;
    
                            exchange.publish(Publish::new(
                                "end".to_string().as_bytes(),
                                QUEUE_POSTS_TO_AVG
                            )).unwrap();
    
                            exchange.publish(Publish::new(
                                "end".to_string().as_bytes(),
                                QUEUE_POSTS_TO_FILTER_SCORE
                            )).unwrap();
                                    
                            logger.info("posts done".to_string());
                        }
                        OPCODE_COMMENT_END => {
                            comments_done = true;
                            logger.info("comments done".to_string());
                        }
                        OPCODE_POST => {
                            let posts = Post::deserialize_multiple(payload.to_string());
    
                            n_post_received = n_post_received + posts.len();
    
                            for post in posts {
                                logger.debug(format!("received post: id {}", post.id));
    
                                exchange.publish(Publish::new(
                                    json!({
                                        "post_id": post.id,
                                        "score": post.score,
                                    }).to_string().as_bytes(),
                                    QUEUE_POSTS_TO_AVG
                                )).unwrap();
                            }
    
                            logger.info(format!("n post received: {}", n_post_received))
                            
                            /* 
                            exchange.publish(Publish::new(
                                json!({
                                    "post_id": post.id,
                                    "score": post.score,
                                    "url": post.url,
                                }).to_string().as_bytes(),
                                QUEUE_POSTS_TO_FILTER_SCORE
                            )).unwrap();
                            */
                        }
                        OPCODE_COMMENT => {
                            /* 
                            let comment = Comment::deserialize(payload.to_string());
    
                            exchange.publish(Publish::new(
                                json!({
                                    "permalink": comment.permalink,
                                    "body": comment.body
                                }).to_string().as_bytes(),
                                QUEUE_COMMENTS_TO_FILTER_STUDENTS
                            )).unwrap();
                            */
    
                        }
                        _ => {}
                    }
                    
                    if posts_done && comments_done {
                        break;
                    }
                }
                _ => {}
            }
        }
    }    
}
