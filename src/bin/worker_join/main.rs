use std::collections::HashMap;

use amiquip::ConsumerMessage;
use reddit_meme_analyzer::commons::{
    constants::queues::{QUEUE_COMMENTS_TO_JOIN, QUEUE_POSTS_TO_JOIN},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_connect, middleware_consumer_end, middleware_create_channel,
            middleware_create_consumer, middleware_create_exchange, middleware_declare_queue,
            Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL,
        },
    },
};

use crate::{
    handlers::{
        handle_comments::{self, handle_comments},
        handle_posts::handle_posts,
    },
    messages::{data_comment::DataComment, data_post_url::DataPostUrl},
};

mod handlers;
mod messages;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue_posts = middleware_declare_queue(&channel, QUEUE_POSTS_TO_JOIN);
    let queue_comments = middleware_declare_queue(&channel, QUEUE_COMMENTS_TO_JOIN);
    let consumer_posts = middleware_create_consumer(&queue_posts);
    let consumer_comments = middleware_create_consumer(&queue_comments);
    let exchange = middleware_create_exchange(&channel);

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

    if connection.close().is_ok() {
        logger.info("connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
