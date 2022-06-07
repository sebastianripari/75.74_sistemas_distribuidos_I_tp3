use std::collections::HashMap;

use crate::{messages::inbound::message_posts::PostData, utils::logger::{Logger, LOG_RATE}};

pub fn handle_posts(
    payload: Vec<PostData>,
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
