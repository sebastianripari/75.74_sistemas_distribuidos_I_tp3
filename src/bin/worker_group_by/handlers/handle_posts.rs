use std::collections::HashMap;

use reddit_meme_analyzer::commons::utils::logger::{Logger, LOG_RATE};

use crate::messages::data_post_url::DataPostUrl;

pub fn handle_posts(
    payload: Vec<DataPostUrl>,
    n: &mut usize,
    logger: &Logger,
    posts: &mut HashMap<String, String>,
) {
    *n += payload.len();

    for post in payload {
        posts.insert(post.post_id, post.url);
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n posts processed: {}", n))
    }
}