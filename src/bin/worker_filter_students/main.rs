use amiquip::ConsumerMessage;
use handlers::handle_comments::handle_comments;
use messages::message_comment_body::DataCommentBody;
use reddit_meme_analyzer::commons::{
    constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_JOIN},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_consumer_end,
            Message, MiddlewareConnection, MESSAGE_OPCODE_END,
            MESSAGE_OPCODE_NORMAL,
        },
    },
};

mod handlers;
mod messages;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);
    {
        let consumer = middleware.get_consumer(QUEUE_COMMENTS_TO_FILTER_STUDENTS);
        let exchange = middleware.get_direct_exchange();

        let mut n_processed = 0;
        let mut n_end = 0;
        let mut end = false;
        for message in consumer.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentBody>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("received end".to_string());
                        end = middleware_consumer_end(
                            &mut n_end,
                            &exchange,
                            [QUEUE_COMMENTS_TO_JOIN].to_vec(),
                            0,
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
        }
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
