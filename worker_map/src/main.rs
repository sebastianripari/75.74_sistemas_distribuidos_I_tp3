use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json};
use std::{env, thread, time::Duration};

use crate::utils::logger::Logger;

mod utils;

#[derive(Deserialize, Debug)]
struct Msg {
    permalink: String,
}

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// queue output
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

const COMMENT_PERMALINK_REGEX: &str = r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

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
            logger.info("connected with rabbitmq".to_string());
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(QUEUE_COMMENTS_TO_MAP, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    let mut n_processed = 0;
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    break;
                }

                let value: Msg = serde_json::from_str(&body).unwrap();
                n_processed = n_processed + 1;
                logger.debug(format!("processing: {:?}", value));
                let permalink = value.permalink;
                let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

                if let Some(captures) = regex.captures(&permalink) {
                    let post_id = captures.get(1).unwrap().as_str();
                    logger.debug(format!("post id found {}", post_id));
                    exchange
                        .publish(Publish::new(
                            json!({ "post_id": post_id }).to_string().as_bytes(),
                            QUEUE_COMMENTS_TO_JOIN,
                        ))
                        .unwrap();
                }

                if n_processed % 1000 == 0 {
                    logger.info(format!("n processed: {}", n_processed));
                }

                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("worker map shutdown".to_string());
}
