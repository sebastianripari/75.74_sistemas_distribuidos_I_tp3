use crate::{utils::logger::Logger};
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions,
};
use handlers::handle_posts_end::handle_post_end;
use handlers::handle_posts::handle_posts;
use handlers::handle_comments_end::handle_comments_end;
use handlers::handle_comments::handle_comments;

use std::{env, thread, time::Duration};

mod entities;
mod messages;
mod utils;
mod handlers;

// queue input
pub const QUEUE_INITIAL_STATE: &str = "QUEUE_INITIAL_STATE";

// queue output
pub const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";
pub const QUEUE_POSTS_TO_FILTER_SCORE: &str = "QUEUE_POSTS_TO_FILTER_SCORE";
pub const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// msg opcodes
const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

pub const LOG_LEVEL: &str = "debug";
pub const LOG_RATE: usize = 100000;

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
            panic!("could not get rabbitmq password from env")
        }
    }

    let mut rabbitmq_connection;
    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            println!("connected with rabbitmq");
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
    let exchange = Exchange::direct(&channel);

    let queue = channel
        .queue_declare(QUEUE_INITIAL_STATE, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    let mut n_post_received: usize = 0;
    let mut n_comment_received: usize = 0;

    let mut posts_end = false;
    let mut comments_end = false;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                
                let splited: Vec<&str> = body.split('|').collect();
                let opcode = splited[0].parse::<u8>().unwrap();
                let payload = splited[1..].join("|");

                match opcode {
                    OPCODE_POST_END => {
                        handle_post_end(&exchange, logger.clone());
                        posts_end = true;
                    }
                    OPCODE_COMMENT_END => {
                        handle_comments_end(&exchange, logger.clone());
                        comments_end = true;
                    }
                    OPCODE_POST => {
                        handle_posts(
                            payload.to_string(),
                            &exchange,
                            &mut n_post_received,
                            logger.clone(),
                        );
                    }
                    OPCODE_COMMENT => {
                        handle_comments(
                            payload.to_string(),
                            &exchange,
                            &mut n_comment_received,
                            logger.clone(),
                        );
                    }
                    _ => logger.info("opcode invalid".to_string()),
                }

                consumer.ack(delivery).unwrap();

                if posts_end && comments_end {
                    logger.info("doing end".to_string());
                    break;
                }
            }
            _ => {}
        }
    }
}
