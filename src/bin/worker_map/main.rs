use amiquip::Exchange;
use handlers::handle_comments::{handle_comments};
use messages::data_comment_body_sentiment::DataCommentBodySentiment;
use reddit_meme_analyzer::commons::{
    constants::queues::{
        QUEUE_COMMENTS_TO_MAP, QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_COMMENTS_TO_FILTER_STUDENTS,
    },
    utils::{
        logger::{logger_create, Logger},
        middleware::{MiddlewareConnection, MiddlewareService, middleware_send_msg_end, get_n_consumers},
    },
};

mod handlers;
mod messages;

struct WorkerMap;

impl MiddlewareService<Vec<DataCommentBodySentiment>> for WorkerMap {
    fn process_message(
        &self,
        payload: &mut Vec<DataCommentBodySentiment>,
        n_processed: &mut usize,
        exchange: &Exchange,
        logger: &Logger
    ) {
        handle_comments(payload, n_processed, &exchange, &logger);
    }

    fn process_end(&self, exchange: &Exchange) {
        middleware_send_msg_end(exchange, QUEUE_COMMENTS_TO_GROUP_BY);
        for _ in 0..get_n_consumers()[0] {
            middleware_send_msg_end(exchange, QUEUE_COMMENTS_TO_FILTER_STUDENTS);  
        }
    }
}

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);
    {
        let consumer = middleware.get_consumer(QUEUE_COMMENTS_TO_MAP);
        let exchange = middleware.get_direct_exchange();

        let worker = WorkerMap;

        worker.consume(&consumer, &exchange, &logger);
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
