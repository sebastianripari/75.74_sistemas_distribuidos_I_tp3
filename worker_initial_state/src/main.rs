use std::{env, thread, time::Duration};

use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use serde_json::{json, Value};

use crate::{entities::comment::Comment, entities::post::Post, utils::logger::Logger};

mod entities;
mod utils;

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

fn handle_post(payload: String, exchange: &Exchange, n_post_received: &mut usize, logger: Logger) {
    let posts = Post::deserialize_multiple(payload);
    let posts_clone = posts.clone();

    *n_post_received = *n_post_received + posts.len();

    let posts_1: Value;
    let posts_2: Value;

    posts_1 = posts
        .into_iter()
        .map(|post| {
            json!({
                "score": post.score
            })
        })
        .rev()
        .collect();

    posts_2 = posts_clone
        .into_iter()
        .map(|post| {
            json!({
                "post_id": post.id,
                "score": post.score,
                "url": post.url,
            })
        })
        .rev()
        .collect();

    exchange
        .publish(Publish::new(
            posts_1.to_string().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();

    exchange
        .publish(Publish::new(
            posts_2.to_string().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();

    if *n_post_received % 10000 == 0 {
        logger.info(format!("n post received: {}", n_post_received))
    }
}

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger.info("start".to_string());

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
    let queue = channel
        .queue_declare(QUEUE_INITIAL_STATE, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    loop {
        let mut n_post_received: usize = 0;
        let mut posts_done = false;
        let mut comments_done = false;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    let splited: Vec<&str> = body.split('|').collect();

                    let opcode = splited[0].parse::<u8>().unwrap();
                    let payload = splited[1..].join("|");

                    match opcode {
                        OPCODE_POST_END => {
                            posts_done = true;

                            exchange
                                .publish(Publish::new(
                                    "end".to_string().as_bytes(),
                                    QUEUE_POSTS_TO_AVG,
                                ))
                                .unwrap();

                            exchange
                                .publish(Publish::new(
                                    "end".to_string().as_bytes(),
                                    QUEUE_POSTS_TO_FILTER_SCORE,
                                ))
                                .unwrap();

                            logger.info("posts done".to_string());
                        }
                        OPCODE_COMMENT_END => {
                            comments_done = true;
                            logger.info("comments done".to_string());
                        }
                        OPCODE_POST => {
                            handle_post(
                                payload.to_string(),
                                &exchange,
                                &mut n_post_received,
                                logger.clone(),
                            );
                        }
                        OPCODE_COMMENT => {
                            let comment = Comment::deserialize(payload.to_string());

                            exchange
                                .publish(Publish::new(
                                    json!({
                                        "permalink": comment.permalink,
                                        "body": comment.body
                                    })
                                    .to_string()
                                    .as_bytes(),
                                    QUEUE_COMMENTS_TO_FILTER_STUDENTS,
                                ))
                                .unwrap();
                        }
                        _ => {}
                    }

                    consumer.ack(delivery).unwrap();

                    if posts_done && comments_done {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
}
