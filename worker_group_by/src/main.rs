use std::{collections::HashMap, env, thread, time::Duration};

use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use handlers::{handle_comments::handle_comments, handle_posts::handle_posts};
use messages::{
    inbound::{message_comments::MessageInboundComments, message_posts::MessageInboundPosts},
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL},
};
use utils::logger::Logger;

mod handlers;
mod messages;
mod utils;

pub const LOG_RATE: usize = 100000;
const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_GROUP_BY: &str = "QUEUE_POSTS_TO_GROUP_BY";
const QUEUE_COMMENTS_TO_GROUP_BY: &str = "QUEUE_COMMENTS_TO_GROUP_BY";

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

    let queue_posts = channel
        .queue_declare(QUEUE_POSTS_TO_GROUP_BY, QueueDeclareOptions::default())
        .unwrap();
    let consumer_posts = queue_posts.consume(ConsumerOptions::default()).unwrap();
    
    let queue = channel
        .queue_declare(QUEUE_COMMENTS_TO_GROUP_BY, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    
    let mut end = false;
    let mut n_posts_processed = 0;
    let mut posts = HashMap::new();
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundPosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(
                            payload.unwrap(),
                            &mut n_posts_processed,
                            &logger,
                            &mut posts
                        );
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    let mut n_comments_processed = 0;
    let mut comments = HashMap::new();
    end = false;
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_comments_processed,
                            &logger,
                            &posts,
                            &mut comments
                        );
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    logger.info("finding max".to_string());

    let max = comments
        .iter()
        .max_by(|a, b| (a.1.1 / (a.1.0 as f32)).partial_cmp(&(b.1.1 / (b.1.0 as f32))).unwrap_or(std::cmp::Ordering::Equal));

    logger.info(format!("max is: {:?}", max.unwrap()));

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
