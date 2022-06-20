use amiquip::ConsumerMessage;
use handlers::{handle_posts::{handle_posts}, handle_score_avg::{handle_score_avg}};
use messages::data_post_score_url::DataPostScoreUrl;
use reddit_meme_analyzer::commons::{utils::{logger::logger_create, middleware::{middleware_connect, middleware_create_channel, middleware_declare_queue, middleware_create_consumer, middleware_create_exchange, Message, middleware_consumer_end, MESSAGE_OPCODE_NORMAL, MESSAGE_OPCODE_END}}, constants::queues::{QUEUE_POSTS_TO_FILTER_SCORE, AVG_TO_FILTER_SCORE}};

mod messages;
mod handlers;

type PostTuple = (String, i32, String);

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue_posts = middleware_declare_queue(&channel, QUEUE_POSTS_TO_FILTER_SCORE);
    let queue_score_avg = middleware_declare_queue(&channel, AVG_TO_FILTER_SCORE);
    let consumer_posts = middleware_create_consumer(&queue_posts);
    let consumer_score_avg = middleware_create_consumer(&queue_score_avg);
    let exchange = middleware_create_exchange(&channel);

    let mut n_end_posts = 0;
    let mut n_end_avg = 0;
    let mut n_processed = 0;
    let mut posts = Vec::new();
    
    let mut end = false;

    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataPostScoreUrl>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        if middleware_consumer_end(&mut n_end_posts, &exchange, [].to_vec(), 0) {
                            end = true;
                        }
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
            _ => {}
        }
    }

    end = false;
    for message in consumer_score_avg.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<f32> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        if middleware_consumer_end(&mut n_end_avg, &exchange, [].to_vec(), 1) {
                            end = true;
                        }
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_score_avg(payload.unwrap(), &logger, &mut posts, &exchange);
                    }
                    _ => {}
                }

                consumer_score_avg.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    if connection.close().is_ok() {
        logger.info("connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
