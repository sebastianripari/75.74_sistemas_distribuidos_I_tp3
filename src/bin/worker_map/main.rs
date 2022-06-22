use amiquip::ConsumerMessage;
use handlers::handle_comments::handle_comments;
use messages::data_comment_body_sentiment::DataCommentBodySentiment;
use reddit_meme_analyzer::commons::{
    constants::queues::{
        QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_COMMENTS_TO_MAP,
    },
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
        let consumer = middleware.get_consumer(QUEUE_COMMENTS_TO_MAP);
        let exchange = middleware.get_direct_exchange();

        let mut n_end = 0;
        let mut n_processed = 0;

        for message in consumer.receiver().iter() {
            let mut end = false;

            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentBodySentiment>> =
                    serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(
                            &mut n_end,
                            &exchange,
                            [
                                QUEUE_COMMENTS_TO_FILTER_STUDENTS,
                                QUEUE_COMMENTS_TO_GROUP_BY,
                            ]
                            .to_vec(),
                            0,
                        );
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            &mut payload.unwrap(),
                            &mut n_processed,
                            &exchange,
                            &logger,
                        );
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
