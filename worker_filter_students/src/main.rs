use amiquip::{ConsumerMessage};
use constants::queues::QUEUE_COMMENTS_TO_FILTER_STUDENTS;
use messages::{
    inbound::{data_comment_body::{DataCommentBody}},
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}
};
use handlers::handle_comments::handle_comments;
use handlers::handle_comments_end::handle_comments_end;
use utils::{logger::logger_create, middleware::{middleware_connect, middleware_create_channel, middleware_declare_queue, middleware_create_consumer, middleware_create_exchange, Message}};

mod messages;
mod utils;
mod handlers;
mod constants;

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
                        end = handle_comments_end(
                            &mut n_end,
                            &exchange,
                            &logger
                        );
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

    connection.close().unwrap();

    logger.info("shutdown".to_string());
}
