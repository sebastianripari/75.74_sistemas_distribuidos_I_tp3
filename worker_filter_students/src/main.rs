use amiquip::ConsumerMessage;
use constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_JOIN};
use handlers::handle_comments::handle_comments;
use messages::{
    inbound::data_comment_body::DataCommentBody,
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL},
};
use utils::{
    logger::logger_create,
    middleware::{
        middleware_connect, middleware_consumer_end, middleware_create_channel,
        middleware_create_consumer, middleware_create_exchange, middleware_declare_queue, Message,
    },
};

mod constants;
mod handlers;
mod messages;
mod utils;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue = middleware_declare_queue(&channel, QUEUE_COMMENTS_TO_FILTER_STUDENTS);
    let consumer = middleware_create_consumer(&queue);
    let exchange = middleware_create_exchange(&channel);

    let mut n_processed = 0;
    let mut n_end = 0;
    let mut end = false;
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentBody>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(
                            &mut n_end,
                            &exchange,
                            [QUEUE_COMMENTS_TO_JOIN].to_vec(),
                        );
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(payload.unwrap(), &mut n_processed, &logger, &exchange);
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {
                break;
            }
        }
    }

    if connection.close().is_ok() {
        logger.info("connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
