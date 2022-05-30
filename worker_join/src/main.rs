use std::{env, thread, time::Duration};
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions,
};
use serde::{Deserialize};

use crate::{utils::logger::Logger};

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

    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
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

    let mut n_received = 0;
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

                n_received = n_received + 1;

                let value: Msg = serde_json::from_str(&body).unwrap();

                if n_received % 10000 == 0 {
                    println!("processing: post id {}", value.post_id);
                }

                consumer_posts.ack(delivery).unwrap();
            }
            _ => logger.info("error consuming".to_string()),
        }
    }


    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
