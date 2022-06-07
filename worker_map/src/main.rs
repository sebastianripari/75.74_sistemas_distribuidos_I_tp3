use amiquip::{
    ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions,
};
use messages::inbound::message_comments::MessageInboundComments;
use messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use utils::logger::logger_create;
use utils::rabbitmq::rabbitmq_connect;
use std::{thread, time::Duration};
use handlers::handle_comments_end::handle_comments_end;
use handlers::handle_comments::handle_comments;

mod messages;
mod utils;
mod handlers;

pub const LOG_RATE: usize = 100000;

// queue input
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// queue output
const QUEUE_COMMENTS_TO_FILTER_STUDENTS: &str = "QUEUE_COMMENTS_TO_FILTER_STUDENTS";
const QUEUE_COMMENTS_TO_GROUP_BY: &str = "QUEUE_COMMENTS_TO_GROUP_BY";

fn main() {
    let logger = logger_create();

    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(QUEUE_COMMENTS_TO_MAP, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    let mut n_processed = 0;
    let mut end = false;
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        handle_comments_end(&exchange, &logger);
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            &mut payload.unwrap(),
                            &mut n_processed,
                            &exchange,
                            &logger
                        );
                    }
                    _ => {
                    }
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

    logger.info("worker map shutdown".to_string());
}
