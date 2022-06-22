use std::collections::HashMap;

use amiquip::Exchange;
use reddit_meme_analyzer::commons::{utils::{logger::{Logger, LOG_RATE}, middleware::middleware_send_msg}, constants::queues::QUEUE_TO_CLIENT};

use crate::messages::{data_post_url::{DataPostUrlOutbound}, data_comment::DataComment};

pub fn handle_comments(
    payload: DataComment,
    n: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
    exchange: &Exchange
) {
    *n += 1;

    if let Some(post_url) = posts.get(&payload.post_id) {

        let payload = DataPostUrlOutbound {
            key: "best_students_memes_url".to_string(),
            value: post_url.to_string(),
        };

        middleware_send_msg(exchange, &payload, QUEUE_TO_CLIENT);
    }

    posts.remove(&payload.post_id);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n comments processed: {}", n));
    }
}