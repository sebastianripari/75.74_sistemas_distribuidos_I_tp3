use amiquip::{Exchange};

use crate::{
    utils::{logger::Logger, middleware::middleware_send_msg_end},
    QUEUE_POSTS_TO_AVG, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY,
};

fn publish_end_scores(exchange: &Exchange) {
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_AVG);
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_GROUP_BY);
}

fn publish_end_posts(exchange: &Exchange) {
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_FILTER_SCORE);
}

pub fn handle_post_end(exchange: &Exchange, logger: Logger) {
    publish_end_scores(exchange);
    publish_end_posts(exchange);

    logger.info("posts done".to_string());
}
