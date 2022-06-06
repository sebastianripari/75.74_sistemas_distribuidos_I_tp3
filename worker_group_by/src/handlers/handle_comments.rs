use std::collections::HashMap;
use crate::{messages::inbound::message_comments::DataInboundComment, utils::logger::Logger, LOG_RATE};

pub fn handle_comments(
    payload: Vec<DataInboundComment>,
    n: &mut usize,
    logger: &Logger,
    comments: &mut HashMap<String, (usize, f32)>,
) {
    *n += payload.len();

    for comment in payload {
        logger.debug(format!("processing: {}", comment.post_id));
        if let Some((count, sum)) = comments.get_mut(&comment.post_id) {
            *count = *count + 1;
            *sum = *sum + comment.sentiment;
        } else {
            comments.insert(comment.post_id, (1, comment.sentiment));
        }
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n))
    }
}
