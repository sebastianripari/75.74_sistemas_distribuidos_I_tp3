use std::collections::HashMap;
use amiquip::{Exchange};
use crate::{
    messages::{
       inbound::data_comment::DataComment,
       outbound::data_post_url::DataPostUrl
    },
    utils::{logger::{Logger, LOG_RATE}, middleware::middleware_send_msg}, constants::queues::QUEUE_TO_CLIENT,
};

pub fn handle_comments(
    payload: DataComment,
    n: &mut usize,
    n_joins: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
    exchange: &Exchange
) {
    *n += 1;

    if let Some(post_url) = posts.get(&payload.post_id) {
        logger.debug(format!(
            "join ok, id: {}, url: {}",
            payload.post_id, post_url
        ));

        let payload = DataPostUrl {
            key: "best_students_memes_url".to_string(),
            value: post_url.to_string(),
        };

        middleware_send_msg(exchange, &payload, QUEUE_TO_CLIENT);

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
