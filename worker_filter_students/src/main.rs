use amiquip::{ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions};
use constants::queues::QUEUE_COMMENTS_TO_FILTER_STUDENTS;
use messages::{
    inbound::{message_comments::MessageInboundComments},
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}
};
use handlers::handle_comments::handle_comments;
use handlers::handle_comments_end::handle_comments_end;
use utils::{rabbitmq::rabbitmq_connect, logger::logger_create};

use std::{thread, time::Duration};

mod messages;
mod utils;
mod handlers;
mod constants;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
            QueueDeclareOptions::default(),
        )
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
                        handle_comments_end(
                            &exchange,
                            &logger
                        );
                        end = true
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_processed,
                            &logger,
                            &exchange
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
