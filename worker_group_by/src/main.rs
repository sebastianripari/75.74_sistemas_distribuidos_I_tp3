use amiquip::{ConsumerMessage};
use constants::queues::{QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_POSTS_TO_GROUP_BY, QUEUE_TO_CLIENT};
use handlers::{handle_comments::handle_comments, handle_posts::handle_posts};
use messages::{
    inbound::{data_comment_sentiment::DataCommentSentiment, data_post_url::DataPostUrl},
    outbound::data_best_url::DataBestUrl
};
use std::{collections::HashMap};
use utils::{logger::logger_create, middleware::{middleware_connect, middleware_create_channel, middleware_declare_queue, middleware_create_consumer, middleware_create_exchange, middleware_send_msg, Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}};

mod constants;
mod handlers;
mod messages;
mod utils;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue_posts = middleware_declare_queue(&channel, QUEUE_POSTS_TO_GROUP_BY);
    let queue_comments = middleware_declare_queue(&channel, QUEUE_COMMENTS_TO_GROUP_BY);
    let consumer_posts = middleware_create_consumer(&queue_posts);
    let consumer_comments = middleware_create_consumer(&queue_comments);
    let exchange = middleware_create_exchange(&channel);

    let mut end = false;
    let mut n_posts_processed = 0;
    let mut posts = HashMap::new();
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataPostUrl>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(
                            payload.unwrap(),
                            &mut n_posts_processed,
                            &logger,
                            &mut posts,
                        );
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    let mut n_comments_processed = 0;
    let mut comments = HashMap::new();
    end = false;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataCommentSentiment>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_comments_processed,
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
            _ => {}
        }
    }

    logger.info("finding max".to_string());
    let max = comments.iter().max_by(|a, b| {
        (a.1 .1 / (a.1 .0 as f32))
            .partial_cmp(&(b.1 .1 / (b.1 .0 as f32)))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let url = max.unwrap().1 .2.to_string();

    let payload = DataBestUrl {
        key: "meme_with_best_sentiment".to_string(),
        value: url,
    }; 

    middleware_send_msg(&exchange, &payload, QUEUE_TO_CLIENT);

    connection.close().unwrap();

    logger.info("shutdown".to_string());
}
