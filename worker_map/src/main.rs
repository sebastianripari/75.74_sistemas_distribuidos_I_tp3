use amiquip::ConsumerMessage;
use constants::queues::QUEUE_COMMENTS_TO_MAP;
use handlers::handle_comments::handle_comments;
use handlers::handle_comments_end::handle_comments_end;
use messages::inbound::message_comments::MessageInboundComments;
use messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use std::{thread, time::Duration};
use utils::logger::logger_create;
use utils::rabbitmq::{
    rabbitmq_connect, rabbitmq_create_channel, rabbitmq_create_consumer, rabbitmq_create_exchange,
    rabbitmq_declare_queue, rabbitmq_end_reached,
};

mod constants;
mod handlers;
mod messages;
mod utils;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_create_channel(&mut connection);
    let queue = rabbitmq_declare_queue(&channel, QUEUE_COMMENTS_TO_MAP);
    let consumer = rabbitmq_create_consumer(&queue);
    let exchange = rabbitmq_create_exchange(&channel);

    let mut n_end = 0;
    let mut n_processed = 0;

    for message in consumer.receiver().iter() {
        let mut end = false;

        if let ConsumerMessage::Delivery(delivery) = message {
            let body = String::from_utf8_lossy(&delivery.body);
            let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
            let opcode = msg.opcode;
            let payload = msg.payload;

            if opcode == MESSAGE_OPCODE_END {
                if rabbitmq_end_reached(&mut n_end) {
                    handle_comments_end(&exchange, &logger);
                    end = true;
                }
            }
            if opcode == MESSAGE_OPCODE_NORMAL {
                handle_comments(&mut payload.unwrap(), &mut n_processed, &exchange, &logger);
            }

            consumer.ack(delivery).unwrap();

            if end {
                break;
            }
        }
    }

    connection.close().unwrap();

    logger.info("shutdown".to_string());
}
