use amiquip::{ConsumerMessage};
use std::{collections::HashMap};
use crate::constants::queues::{QUEUE_POSTS_TO_JOIN, QUEUE_COMMENTS_TO_JOIN};
use crate::handlers::handle_comments::handle_comments;
use crate::handlers::handle_posts::handle_posts;
use crate::messages::inbound::data_post_url::DataPostUrl;
use crate::messages::inbound::data_comment::DataComment;
use crate::messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use crate::utils::logger::logger_create;
use crate::utils::middleware::{middleware_connect, middleware_create_channel, middleware_declare_queue, middleware_create_consumer, middleware_create_exchange, Message, middleware_consumer_end};

mod entities;
mod handlers;
mod messages;
mod utils;
mod constants;

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

    let mut n_post_processed = 0;
    let mut posts = HashMap::new();
    let mut end = false;
    let mut n_end = 0;
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataPostUrl>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end, &exchange, [].to_vec(), 0);
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(payload.unwrap(), &mut n_post_processed, &mut posts, &logger)
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {
                break;
            },
        }
    }

    let mut n_comments_processed = 0;
    let mut n_joins = 0;

    end = false;
    n_end = 0;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<DataComment> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end, &exchange, [].to_vec(), 1);
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_comments_processed,
                            &mut n_joins,
                            &mut posts,
                            &logger,
                            &exchange
                        );
                    }
                    _ => {}
                }

                consumer_comments.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {
                break;
            },
        }
    }

    if connection.close().is_ok() {
        logger.info("connection closed".to_string());
    }

    println!("worker join shutdown");
}
