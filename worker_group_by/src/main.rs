use std::{env, thread, time::Duration, collections::HashMap};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};
use messages::{inbound::message_comments::MessageInboundComments, opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}};
use utils::logger::Logger;
use handlers::handle_comments::handle_comments;

mod utils;
mod messages;
mod handlers;

pub const LOG_RATE: usize = 100000;
const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_COMMENTS_TO_GROUP_BY: &str = "QUEUE_COMMENTS_TO_GROUP_BY";

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

    rabbitmq_connection
}

fn main() {
    let logger = logger_start();

    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(
            QUEUE_COMMENTS_TO_GROUP_BY,
            QueueDeclareOptions::default(),
        )
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    
    let mut n_processed = 0;
    let mut comments = HashMap::new();
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {}
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_processed,
                            &logger,
                            &mut comments
                        );
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
