use amiquip::ConsumerMessage;
use handlers::{handle_comments::{self, handle_comments}, handle_posts::{self, handle_posts}};
use reddit_meme_analyzer::commons::{utils::{logger::logger_create, middleware::{middleware_connect, middleware_create_channel, middleware_declare_queues, middleware_declare_queue, middleware_create_consumer, middleware_create_exchange, Message, middleware_consumer_end, middleware_end_reached, middleware_send_msg_end, MESSAGE_OPCODE_NORMAL, MESSAGE_OPCODE_END}}, constants::{queues::{QUEUES, QUEUE_INITIAL_STATE, QUEUE_COMMENTS_TO_MAP, QUEUE_POSTS_TO_AVG, QUEUE_POSTS_TO_GROUP_BY, QUEUE_POSTS_TO_FILTER_SCORE}, opcodes::{OPCODE_POST_END, OPCODE_POST, OPCODE_COMMENT}}};

mod handlers;
mod messages;

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

    let mut n_end_posts = 0;
    let mut n_end_comments = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<String> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(
                            &mut n_end_comments,
                            &exchange,
                            [QUEUE_COMMENTS_TO_MAP].to_vec(),
                            0
                        );
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        let payload_ = payload.unwrap();
                        let splited: Vec<&str> = payload_.split('|').collect();
                        let opcode = splited[0].parse::<u8>().unwrap();
                        let payload = splited[1..].join("|");

                        match opcode {
                            OPCODE_POST_END => {
                                if middleware_end_reached(&mut n_end_posts, 0) {
                                    middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_AVG);
                                    middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_GROUP_BY);
                                    middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_FILTER_SCORE);
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
                    }
                    _ => {}
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
        logger.info("connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
