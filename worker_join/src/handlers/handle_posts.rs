use std::collections::HashMap;

use crate::{messages::inbound::data_post_url::DataPostUrl, utils::logger::{Logger, LOG_RATE}};

pub fn handle_posts(
    payload: Vec<DataPostUrl>,
    n: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
) {
    *n += payload.len();

    for value in payload {
        if value.url != "" {
            posts.insert(value.post_id, value.url);
        }
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n post processed: {}", n));
    }
}
