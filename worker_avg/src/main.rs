use crate::utils::logger::Logger;
use amiquip::{Connection, ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions};
use handlers::handle_calc_avg::handle_calc_avg;
use handlers::handle_calc_avg_end::handle_calc_avg_end;
use messages::{
    message_scores::MessageScores,
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL},
};
use std::{env, thread, time::Duration};

mod handlers;
mod messages;
mod utils;

pub const LOG_RATE: usize = 100000;
const LOG_LEVEL: &str = "debug";

// queue input
pub const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";

// queue output
pub const AVG_TO_FILTER_SCORE: &str = "AVG_TO_FILTER_SCORE";
pub const QUEUE_TO_CLIENT: &str = "QUEUE_TO_CLIENT";

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

fn logger_start() -> Logger {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }

    let logger = Logger::new(log_level);

    logger
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
        .queue_declare(QUEUE_POSTS_TO_AVG, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    let mut n_processed: usize = 0;
    let mut end = false;

    let mut score_count: usize = 0;
    let mut score_sum: u64 = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageScores = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        handle_calc_avg_end(&exchange, &logger, score_sum, score_count);
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_calc_avg(
                            payload.unwrap(),
                            &mut n_processed,
                            &logger,
                            &mut score_count,
                            &mut score_sum,
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

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
