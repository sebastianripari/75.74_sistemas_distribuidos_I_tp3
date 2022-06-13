use amiquip::{ConsumerMessage};
use constants::queues::{QUEUE_COMMENTS_TO_MAP, QUEUE_INITIAL_STATE, QUEUE_POSTS_TO_AVG,
    QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY, QUEUES,
};
use handlers::handle_comments::handle_comments;
use handlers::handle_posts::handle_posts;
use utils::{
    logger::logger_create,
    middleware::{
        middleware_connect, middleware_consumer_end, middleware_create_channel,
        middleware_create_consumer, middleware_create_exchange, middleware_declare_queue,
        middleware_send_msg_end, middleware_declare_queues,
    },
};

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

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    middleware_declare_queues(&channel, QUEUES.to_vec());
    let queue = middleware_declare_queue(&channel, QUEUE_INITIAL_STATE);
    let consumer = middleware_create_consumer(&queue);
    let exchange = middleware_create_exchange(&channel);

    let mut n_post_received: usize = 0;
    let mut n_comment_received: usize = 0;

    let mut end = false;
    let mut n_end = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let splited: Vec<&str> = body.split('|').collect();
                let opcode = splited[0].parse::<u8>().unwrap();
                let payload = splited[1..].join("|");

                match opcode {
                    OPCODE_POST_END => {
                        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_AVG);
                        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_GROUP_BY);
                        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_FILTER_SCORE);
                    }
                    OPCODE_COMMENT_END => {
                        if middleware_consumer_end(
                            &mut n_end,
                            &exchange,
                            [QUEUE_COMMENTS_TO_MAP].to_vec(),
                        ) {
                            end = true;
                        }
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

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    connection.close().unwrap();

    logger.info("shutdown".to_string());
}
