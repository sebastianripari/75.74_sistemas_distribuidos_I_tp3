use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};

use serde_json::{json, Value};
use std::{env, thread, time::Duration};

use crate::utils::logger::Logger;

mod utils;

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";

// queue output
const AVG_TO_FILTER_SCORE: &str = "AVG_TO_FILTER_SCORE";
const QUEUE_TO_CLIENT: &str = "QUEUE_TO_CLIENT";

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
            panic!("could not get rabbitmq user from env")
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
        .queue_declare(QUEUE_POSTS_TO_AVG, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    let mut score_count: u64 = 0;
    let mut score_sum: u64 = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    logger.info("doing stop".to_string());
                    consumer.ack(delivery).unwrap();
                    break;
                }

                if body == "end" {
                    logger.info("doing end".to_string());

                    exchange
                        .publish(Publish::new(
                            json!({ "score_avg": score_sum / score_count })
                                .to_string()
                                .as_bytes(),
                            AVG_TO_FILTER_SCORE,
                        ))
                        .unwrap();

                    exchange
                        .publish(Publish::new(
                            json!({ "score_avg": score_sum / score_count })
                                .to_string()
                                .as_bytes(),
                            QUEUE_TO_CLIENT,
                        ))
                        .unwrap();

                    consumer.ack(delivery).unwrap();

                    score_count = 0;
                    score_sum = 0;

                    continue;
                }

                let deserialized: Value = serde_json::from_str(&body).unwrap();
                let array = deserialized.as_array().unwrap();

                for value in array {
                    logger.debug(format!("processing: {}", value));
                    let score = value["score"].to_string();

                    match score.parse::<i32>() {
                        Ok(score) => {
                            score_count = score_count + 1;
                            score_sum = score_sum + score as u64;
                        }
                        Err(err) => logger.info(format!("error: {}", err)),
                    }
                    if score_count % 10000 == 0 {
                        logger.info(format!("n processed: {}", score_count));
                    }
                }

                consumer.ack(delivery).unwrap();
            }
            _ => {
                logger.info("error consuming".to_string());
                break;
            }
        }
    }
    logger.info("stop consuming".to_string());

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
