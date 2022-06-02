use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use serde::Deserialize;
use serde_json::{Error};
use std::{collections::HashMap, env, thread, time::Duration};

use crate::utils::logger::Logger;

mod entities;
mod utils;

#[derive(Deserialize, Debug)]
struct Msg {
    post_id: String,
    url: String,
}

#[derive(Deserialize, Debug)]
struct MsgComment {
    post_id: String
}

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

// queue output

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
            panic!("could not get rabbitmq password user from env")
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
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    break;
                }

                if body == "end" {
                    logger.info("doing end posts".to_string());
                    consumer_posts.ack(delivery).unwrap();
                    break;
                }

                let array: Vec<Msg> = serde_json::from_str(&body).unwrap();
                n_post_processed = n_post_processed + array.len();

                for value in array {
                    posts.insert(value.post_id, value.url);
                }

                if n_post_processed % 10000 == 0 {
                    logger.info(format!("n post processed: {}", n_post_processed));
                }

                consumer_posts.ack(delivery).unwrap();
            }
            _ => logger.info("error consuming".to_string()),
        }
    }

    let mut n_comments_processed = 0;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    break;
                }
                if body == "end" {
                    consumer_posts.ack(delivery).unwrap();
                    break;
                }

                n_comments_processed = n_comments_processed + 1;

                let value_result: Result<MsgComment, Error> = serde_json::from_str(&body);
                if let Ok(value) = value_result {
                    if n_comments_processed % 1000 == 0 {
                        logger.info(format!("n comments processed: {}", n_comments_processed));
                    }
    
                    if let Some(post_url) = posts.get(&value.post_id) {
                        logger.info(format!("join ok, url: {}", post_url))
                    }
                }

                consumer_comments.ack(delivery).unwrap();
            }
            _ => logger.info("error consuming".to_string()),
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
