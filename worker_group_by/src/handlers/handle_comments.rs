use std::collections::HashMap;
use crate::{messages::inbound::message_comments::DataInboundComment, utils::logger::{Logger, LOG_RATE}};

pub fn handle_comments(
    payload: Vec<DataInboundComment>,
    n: &mut usize,
    logger: &Logger,
    posts: &HashMap<String, String>,
    comments: &mut HashMap<String, (usize, f32, String)>,
) {
    *n += payload.len();

    for comment in payload {
        logger.debug(format!("processing: {}", comment.post_id));
        if let Some(url) = posts.get(&comment.post_id) {
            if url != "" {
                if let Some((count, sum, _)) = comments.get_mut(&comment.post_id) {
                    *count = *count + 1;
                    *sum = *sum + comment.sentiment;
                } else {
                    comments.insert(comment.post_id, (1, comment.sentiment, url.to_string()));
                }
            }
        }
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n))
    }
}
