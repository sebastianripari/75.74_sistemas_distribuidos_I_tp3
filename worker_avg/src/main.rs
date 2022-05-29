use std::{time::Duration, thread, env};
use serde_json::{Value, json};
use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Publish, Exchange};

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

    let mut stop = false;

    // wait rabbit
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
    let queue = channel.queue_declare(QUEUE_POSTS_TO_AVG, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    loop {
        if stop {
            break;
        }

        let mut score_count: u64 = 0;
        let mut score_sum: u64 = 0;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    if body == "stop" {
                        logger.info("doing stop".to_string());
                        stop = true;
                        consumer.ack(delivery).unwrap();
                        break;
                    }

                    if body == "end" {
                        logger.info("doing end".to_string());

                        exchange.publish(Publish::new(
                            json!({
                                "score_avg": score_sum / score_count
                            }).to_string().as_bytes(),
                            AVG_TO_FILTER_SCORE
                        )).unwrap();

                        exchange.publish(Publish::new(
                            json!({
                                "score_avg": score_sum / score_count
                            }).to_string().as_bytes(),
                            QUEUE_TO_CLIENT
                        )).unwrap();

                        consumer.ack(delivery).unwrap();
                        break;
                    }

                    let value: Value = serde_json::from_str(&body).unwrap();
                    logger.debug(format!("processing: {}", value));
                    let score = value["score"].to_string();
                    match score.parse::<i32>() {
                        Ok(score) => {
                            score_count = score_count + 1;
                            score_sum = score_sum + score as u64;
                        }
                        Err(err) => {
                            logger.info(format!("error: {}", err))
                        }
                    }
                    if score_count % 10000 == 0 {
                        logger.info(format!("n processed: {}", score_count));
                    }
                    consumer.ack(delivery).unwrap();
                }
                _ => {
                    logger.info("stop consuming".to_string());
                    break;
                }
            }
        }
        logger.info("stop consuming".to_string());
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
