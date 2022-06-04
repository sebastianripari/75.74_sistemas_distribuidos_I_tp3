use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use serde::Deserialize;
use std::{collections::HashMap, env, thread, time::Duration};

use crate::handlers::handle_comments::handle_comments;
use crate::handlers::handle_posts::handle_posts;
use crate::messages::inbound::message_comments::MessageComments;
use crate::messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use crate::{messages::inbound::message_posts::MessagePosts, utils::logger::Logger};

mod entities;
mod handlers;
mod messages;
mod utils;

#[derive(Deserialize, Debug)]
struct Msg {
    post_id: String,
    url: String,
}

pub const LOG_RATE: usize = 10000;
const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

// queue output

fn logger_start() -> Logger {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger
}

fn rabbitmq_connect(logger: &Logger) -> Connection {
    let rabbitmq_user;
    match env::var("RABBITMQ_USER") {
        Ok(value) => rabbitmq_user = value,
        Err(_) => {
            panic!("could not get rabbitmq user from env")
        }
    }

    let rabbitmq_password;
    match env::var("RABBITMQ_PASSWORD") {
        Ok(value) => rabbitmq_password = value,
        Err(_) => {
            panic!("could not get rabbitmq password user from env")
        }
    }

    let rabbitmq_connection;
    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            logger.info("connected with rabbitmq".to_string());
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    rabbitmq_connection
}

fn main() {
    let logger = logger_start();

    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();

    let queue_posts_to_join = channel
        .queue_declare(QUEUE_POSTS_TO_JOIN, QueueDeclareOptions::default())
        .unwrap();
    let consumer_posts = queue_posts_to_join
        .consume(ConsumerOptions::default())
        .unwrap();

    let queue_comments_to_join = channel
        .queue_declare(QUEUE_COMMENTS_TO_JOIN, QueueDeclareOptions::default())
        .unwrap();
    let consumer_comments = queue_comments_to_join
        .consume(ConsumerOptions::default())
        .unwrap();

    let mut n_post_processed = 0;
    let mut posts = HashMap::new();
    let mut end = false;
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessagePosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("doing end posts".to_string());
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(payload.unwrap(), &mut n_post_processed, &mut posts, &logger)
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => logger.info("error consuming".to_string()),
        }
    }

    let mut n_comments_processed = 0;
    let mut n_joins = 0;
    let mut end = false;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("doing end".to_string());
                        logger.info(format!("n joins: {}", n_joins));
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_comments_processed,
                            &mut n_joins,
                            &mut posts,
                            &logger,
                        );
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => logger.info("error consuming".to_string()),
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
