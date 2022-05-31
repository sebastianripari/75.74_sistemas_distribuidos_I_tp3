use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use serde::Deserialize;
use std::{collections::HashMap, env, thread, time::Duration};

use crate::utils::logger::Logger;

mod entities;
mod utils;

#[derive(Deserialize, Debug)]
struct Msg {
    post_id: String,
    url: String,
}

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";

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
        .queue_declare(QUEUE_POSTS_TO_JOIN, QueueDeclareOptions::default())
        .unwrap();
    let consumer_comments = queue_comments_to_join
        .consume(ConsumerOptions::default())
        .unwrap();

    let mut n_posts_received = 0;
    let mut posts = HashMap::new();
    for message in consumer_posts.receiver().iter() {
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

                n_posts_received = n_posts_received + 1;

                let value: Msg = serde_json::from_str(&body).unwrap();

                if n_posts_received % 10000 == 0 {
                    println!("processing: post id {}", value.post_id);
                }

                posts.insert(value.post_id, value.url);

                consumer_posts.ack(delivery).unwrap();
            }
            _ => logger.info("error consuming".to_string()),
        }
    }

    let mut n_comments_received = 0;
    for message in consumer_posts.receiver().iter() {
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

                n_comments_received = n_comments_received + 1;

                let value: Msg = serde_json::from_str(&body).unwrap();

                if n_comments_received % 10000 == 0 {
                    println!("processing: comment id {}", value.post_id);
                }

                if let Some(post_url) = posts.get(&value.post_id) {
                    println!("join ok, url: {}", post_url)
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
