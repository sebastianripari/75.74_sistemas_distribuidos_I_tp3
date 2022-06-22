use std::collections::HashMap;

use amiquip::ConsumerMessage;
use reddit_meme_analyzer::commons::{
    constants::queues::{QUEUE_COMMENTS_TO_JOIN, QUEUE_POSTS_TO_JOIN},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_consumer_end,
            Message, MiddlewareConnection, MESSAGE_OPCODE_END,
            MESSAGE_OPCODE_NORMAL,
        },
    },
};

use crate::{
    handlers::{handle_comments::handle_comments, handle_posts::handle_posts},
    messages::{data_comment::DataComment, data_post_url::DataPostUrl},
};

mod handlers;
mod messages;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);
    {
        let consumer_posts = middleware.get_consumer(QUEUE_POSTS_TO_JOIN);
        let consumer_comments = middleware.get_consumer(QUEUE_COMMENTS_TO_JOIN);
        let exchange = middleware.get_direct_exchange();

        let mut posts = HashMap::new();

        let mut n_processed = 0;
        let mut end = false;
        let mut n_end = 0;

        for message in consumer_posts.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataPostUrl>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end, &exchange, [].to_vec(), 0);
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(payload.unwrap(), &mut n_processed, &mut posts, &logger)
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
        }

        end = false;
        n_end = 0;
        n_processed = 0;

        for message in consumer_comments.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<DataComment> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("received end".to_string());
                        end = middleware_consumer_end(&mut n_end, &exchange, [].to_vec(), 1);
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_processed,
                            &mut posts,
                            &logger,
                            &exchange,
                        );
                    }
                    _ => {}
                }

                consumer_comments.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
        }
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
