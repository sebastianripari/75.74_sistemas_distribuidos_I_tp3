use amiquip::ConsumerMessage;
use handlers::handle_comments::{handle_comments};
use messages::data_comment_body_sentiment::DataCommentBodySentiment;
use reddit_meme_analyzer::commons::{
    constants::queues::{
        QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_COMMENTS_TO_MAP,
    },
    utils::{
        logger::logger_create,
        middleware::{
            middleware_connect, middleware_consumer_end, middleware_create_channel,
            middleware_create_consumer, middleware_create_exchange, middleware_declare_queue,
            Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL,
        },
    },
};

mod handlers;
mod messages;

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

        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentBodySentiment>> =
                    serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                if opcode == MESSAGE_OPCODE_END {
                    
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

                if opcode == MESSAGE_OPCODE_NORMAL {
                    handle_comments(&mut payload.unwrap(), &mut n_processed, &exchange, &logger);
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
