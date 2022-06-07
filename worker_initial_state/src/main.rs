use crate::utils::logger::Logger;
use amiquip::{
    Channel, Connection, ConsumerMessage, ConsumerOptions, Exchange, Queue, QueueDeclareOptions,
};
use constants::queues::{
    AVG_TO_FILTER_SCORE, QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY,
    QUEUE_COMMENTS_TO_JOIN, QUEUE_COMMENTS_TO_MAP, QUEUE_INITIAL_STATE, QUEUE_POSTS_TO_AVG,
    QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY, QUEUE_POSTS_TO_JOIN,
};
use handlers::handle_comments::handle_comments;
use handlers::handle_comments_end::handle_comments_end;
use handlers::handle_posts::handle_posts;
use handlers::handle_posts_end::handle_post_end;
use utils::{rabbitmq::rabbitmq_connect, logger::logger_create};
use std::{thread, time::Duration};

mod constants;
mod entities;
mod handlers;
mod messages;
mod utils;

// msg opcodes
const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

pub const LOG_LEVEL: &str = "debug";
pub const LOG_RATE: usize = 100000;

fn rabbitmq_declare_queues(channel: &Channel) {
    for queue in [
        AVG_TO_FILTER_SCORE,
        QUEUE_COMMENTS_TO_FILTER_STUDENTS,
        QUEUE_COMMENTS_TO_GROUP_BY,
        QUEUE_COMMENTS_TO_JOIN,
        QUEUE_COMMENTS_TO_MAP,
        QUEUE_INITIAL_STATE,
        QUEUE_POSTS_TO_AVG,
        QUEUE_POSTS_TO_FILTER_SCORE,
        QUEUE_POSTS_TO_GROUP_BY,
        QUEUE_POSTS_TO_JOIN
    ] {
        channel
            .queue_declare(queue, QueueDeclareOptions::default())
            .unwrap();
    }
}

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);
    rabbitmq_declare_queues(&channel);
    let queue = channel
        .queue_declare(QUEUE_INITIAL_STATE, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    let mut n_post_received: usize = 0;
    let mut n_comment_received: usize = 0;

    let mut posts_end = false;
    let mut comments_end = false;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                let splited: Vec<&str> = body.split('|').collect();
                let opcode = splited[0].parse::<u8>().unwrap();
                let payload = splited[1..].join("|");

                match opcode {
                    OPCODE_POST_END => {
                        handle_post_end(&exchange, logger.clone());
                        posts_end = true;
                    }
                    OPCODE_COMMENT_END => {
                        handle_comments_end(&exchange, logger.clone());
                        comments_end = true;
                    }
                    OPCODE_POST => {
                        handle_posts(
                            payload.to_string(),
                            &exchange,
                            &mut n_post_received,
                            logger.clone(),
                        );
                    }
                    OPCODE_COMMENT => {
                        handle_comments(
                            payload.to_string(),
                            &exchange,
                            &mut n_comment_received,
                            logger.clone(),
                        );
                    }
                    _ => logger.info("opcode invalid".to_string()),
                }

                consumer.ack(delivery).unwrap();

                if posts_end && comments_end {
                    logger.info("doing end".to_string());
                    break;
                }
            }
            _ => {}
        }
    }
}
