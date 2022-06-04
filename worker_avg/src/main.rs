use crate::utils::logger::Logger;
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use messages::message_scores::MessageScores;
use serde_json::json;
use std::{env, thread, time::Duration};

mod messages;
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
    let queue = channel
        .queue_declare(QUEUE_POSTS_TO_AVG, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    let mut score_count: u64 = 0;
    let mut score_sum: u64 = 0;

    let mut n_processed: usize = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageScores = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    0 => {
                        logger.info("doing end".to_string());

                        let json_score_avg = json!({
                            "opcode": 1,
                            "payload": score_sum / score_count
                        });

                        exchange
                            .publish(Publish::new(
                                json_score_avg
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
                        break;
                    }
                    1 => {
                        let scores = payload.unwrap();
                        n_processed = n_processed + scores.len();

                        for score in scores {
                            logger.debug(format!("processing: {}", score));
                            score_count = score_count + 1;
                            score_sum = score_sum + score as u64;
                        }

                        if n_processed % 100000 == 0 {
                            logger.info(format!("n processed: {}", score_count));
                        }
                        consumer.ack(delivery).unwrap();
                    }
                    _ => {
                        consumer.ack(delivery).unwrap();
                    }
                }
            }
            _ => {}
        }
    }
    logger.info("stop consuming".to_string());

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
