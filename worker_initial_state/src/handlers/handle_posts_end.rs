use amiquip::{Exchange};

use crate::{
    utils::{logger::Logger, middleware::middleware_send_msg_end},
    QUEUE_POSTS_TO_AVG, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY,
};

pub fn handle_post_end(exchange: &Exchange, logger: Logger) {
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_AVG);
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_GROUP_BY);
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_FILTER_SCORE);
    logger.info("posts done".to_string());
}
