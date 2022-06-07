use std::collections::HashMap;

use crate::{messages::inbound::message_comments::Data, utils::logger::{Logger, LOG_RATE}};

pub fn handle_comments(
    payload: Data,
    n: &mut usize,
    n_joins: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
) {
    *n += 1;

    if let Some(post_url) = posts.get(&payload.post_id) {
        logger.debug(format!("join ok, id: {}, url: {}", payload.post_id, post_url));
        *n_joins += 1;
        if *n_joins % 100 == 0 {
            logger.info(format!("n joins: {}", n_joins));
        }
    }

    posts.remove(&payload.post_id);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n comments processed: {}", n));
    }
}
