use amiquip::ConsumerMessage;
use constants::queues::{QUEUE_COMMENTS_TO_MAP, QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY};
use handlers::handle_comments::handle_comments;
use messages::inbound::data_comments_body_sentiment::{VecDataCommentBodySentiment};
use utils::logger::logger_create;
use utils::middleware::{
    middleware_connect, middleware_create_channel, middleware_create_consumer,
    middleware_create_exchange, middleware_declare_queue, Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL, middleware_consumer_end,
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
    let queue = middleware_declare_queue(&channel, QUEUE_COMMENTS_TO_MAP);
    let consumer = middleware_create_consumer(&queue);
    let exchange = middleware_create_exchange(&channel);

    let mut n_end = 0;
    let mut n_processed = 0;

    for message in consumer.receiver().iter() {
        let mut end = false;

        if let ConsumerMessage::Delivery(delivery) = message {
            let body = String::from_utf8_lossy(&delivery.body);
            let msg: Message<VecDataCommentBodySentiment> = serde_json::from_str(&body).unwrap();
            let opcode = msg.opcode;
            let payload = msg.payload;

            if opcode == MESSAGE_OPCODE_END {
                logger.info(format!("ending {}", n_end));
                if middleware_consumer_end(
                    &mut n_end,
                    &exchange,
                    [
                        QUEUE_COMMENTS_TO_FILTER_STUDENTS,
                        QUEUE_COMMENTS_TO_GROUP_BY,
                    ].to_vec()
                ) {
                    logger.info("ending".to_string());
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
