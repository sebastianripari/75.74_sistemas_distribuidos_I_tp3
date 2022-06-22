use std::collections::HashMap;

use amiquip::ConsumerMessage;
use handlers::{handle_comments::handle_comments, handle_posts::handle_posts};
use messages::{
    data_best_url::DataBestUrl, data_comment_sentiment::DataCommentSentiment,
    data_post_url::DataPostUrl,
};
use reddit_meme_analyzer::commons::{
    constants::queues::{QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_POSTS_TO_GROUP_BY, QUEUE_TO_CLIENT},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_consumer_end,
            middleware_send_msg, Message, MiddlewareConnection, MESSAGE_OPCODE_END,
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
        let consumer_posts = middleware.get_consumer(QUEUE_POSTS_TO_GROUP_BY);
        let consumer_comments = middleware.get_consumer(QUEUE_COMMENTS_TO_GROUP_BY);
        let exchange = middleware.get_direct_exchange();

        let mut end = false;
        let mut n_end = 0;
        let mut n_processed = 0;
        let mut posts = HashMap::new();

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
                        handle_posts(payload.unwrap(), &mut n_processed, &logger, &mut posts);
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
        }

        let mut comments = HashMap::new();
        n_processed = 0;
        end = false;
        n_end = 0;

        for message in consumer_comments.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentSentiment>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end, &exchange, [].to_vec(), 1);
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_processed,
                            &logger,
                            &posts,
                            &mut comments,
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

        logger.info("finding max".to_string());
        let max = comments.iter().max_by(|a, b| {
            (a.1 .1 / (a.1 .0 as f32))
                .partial_cmp(&(b.1 .1 / (b.1 .0 as f32)))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if let Some(max_value) = max {
            let url = max_value.1 .2.to_string();
            let payload = DataBestUrl {
                key: "meme_with_best_sentiment".to_string(),
                value: url,
            };
            middleware_send_msg(&exchange, &payload, QUEUE_TO_CLIENT);
        }
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
