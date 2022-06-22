use amiquip::ConsumerMessage;
use handlers::{handle_posts::handle_posts, handle_score_avg::handle_score_avg};
use messages::data_post_score_url::DataPostScoreUrl;
use reddit_meme_analyzer::commons::{
    constants::queues::{AVG_TO_FILTER_SCORE, QUEUE_POSTS_TO_FILTER_SCORE},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_consumer_end, Message, MiddlewareConnection, MESSAGE_OPCODE_END,
            MESSAGE_OPCODE_NORMAL,
        },
    },
};

mod handlers;
mod messages;

type PostTuple = (String, i32, String);

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);
    
    {
        let consumer_posts = middleware.get_consumer(QUEUE_POSTS_TO_FILTER_SCORE);
        let consumer_score_avg = middleware.get_consumer(AVG_TO_FILTER_SCORE);
        let exchange = middleware.get_direct_exchange();

        let mut posts = Vec::new();

        let mut n_end_posts = 0;
        let mut n_end_avg = 0;
        let mut n_processed = 0;
        let mut end = false;

        for message in consumer_posts.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<DataPostScoreUrl>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end_posts, &exchange, [].to_vec(), 0);
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

        end = false;
        for message in consumer_score_avg.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<f32> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = middleware_consumer_end(&mut n_end_avg, &exchange, [].to_vec(), 1);
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
        }
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
