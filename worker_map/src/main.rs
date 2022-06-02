use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{env, thread, time::Duration};

use crate::utils::logger::Logger;

mod utils;

#[derive(Deserialize, Debug)]
struct Msg {
    permalink: String,
    body: String,
    sentiment: f32,
}

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// queue output
const QUEUE_COMMENTS_TO_FILTER_STUDENTS: &str = "QUEUE_COMMENTS_TO_FILTER_STUDENTS";

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

                if body == "end" {
                    logger.info("doing end".to_string());
                    exchange
                        .publish(Publish::new(
                            "end".to_string().as_bytes(),
                            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
                        ))
                        .unwrap();
                    consumer.ack(delivery).unwrap();
                    break;
                }

                let array: Vec<Msg> = serde_json::from_str(&body).unwrap();
                n_processed = n_processed + array.len();

                let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

                let array_mapped: Value = array
                    .into_iter()
                    .map(|comment| {
                        let permalink = comment.permalink;

                        if let Some(captures) = regex.captures(&permalink) {
                            let post_id = captures.get(1).unwrap().as_str();
                            json!({
                                "post_id": post_id,
                                "body": comment.body,
                            })
                        } else {
                            json!({
                                "post_id": "",
                                "body": comment.body,
                            })
                        }
                    })
                    .rev()
                    .collect();

                exchange
                    .publish(Publish::new(
                        array_mapped.to_string().as_bytes(),
                        QUEUE_COMMENTS_TO_FILTER_STUDENTS,
                    ))
                    .unwrap();

                if n_processed % 100000 == 0 {
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
