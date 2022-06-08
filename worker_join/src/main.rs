use amiquip::{Exchange, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use std::{collections::HashMap, thread, time::Duration};
use crate::constants::queues::{QUEUE_POSTS_TO_JOIN, QUEUE_COMMENTS_TO_JOIN};
use crate::handlers::handle_comments::handle_comments;
use crate::handlers::handle_posts::handle_posts;
use crate::messages::inbound::message_comments::MessageComments;
use crate::messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use crate::utils::logger::logger_create;
use crate::utils::rabbitmq::rabbitmq_connect;
use crate::{messages::inbound::message_posts::MessagePosts};

mod entities;
mod handlers;
mod messages;
mod utils;
mod constants;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);

    let queue_posts_to_join = channel
        .queue_declare(QUEUE_POSTS_TO_JOIN, QueueDeclareOptions::default())
        .unwrap();
    let consumer_posts = queue_posts_to_join
        .consume(ConsumerOptions::default())
        .unwrap();

    let queue_comments_to_join = channel
        .queue_declare(QUEUE_COMMENTS_TO_JOIN, QueueDeclareOptions::default())
        .unwrap();
    let consumer_comments = queue_comments_to_join
        .consume(ConsumerOptions::default())
        .unwrap();

    let mut n_post_processed = 0;
    let mut posts = HashMap::new();
    let mut end = false;
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessagePosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("doing end posts".to_string());
                        end = true;
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
            _ => logger.info("error consuming".to_string()),
        }
    }

    let mut n_comments_processed = 0;
    let mut n_joins = 0;
    let mut end = false;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        logger.info("doing end".to_string());
                        logger.info(format!("n joins: {}", n_joins));
                        end = true;
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
            _ => logger.info("error consuming".to_string()),
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
