use std::collections::HashMap;

use reddit_meme_analyzer::commons::utils::logger::{Logger, LOG_RATE};

use crate::messages::data_post_url::DataPostUrl;

pub fn handle_posts(
    payload: Vec<DataPostUrl>,
    n: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
) {
    *n += payload.len();

    for value in payload {
        if !value.url.is_empty() {
            posts.insert(value.post_id, value.url);
        }
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n post processed: {}", n));
    }
}