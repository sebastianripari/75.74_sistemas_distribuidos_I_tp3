use crate::{
    constants::queues::QUEUE_COMMENTS_TO_JOIN,
    utils::{logger::Logger, middleware::middleware_consumer_end},
};
use amiquip::Exchange;

pub fn handle_comments_end(n_end: &mut usize, exchange: &Exchange, logger: &Logger) -> bool {
    logger.info("doing end".to_string());

    middleware_consumer_end(n_end, exchange, [QUEUE_COMMENTS_TO_JOIN].to_vec())
}
