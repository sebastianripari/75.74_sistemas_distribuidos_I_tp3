use amiquip::Exchange;
use handlers::handle_comments::handle_comments;
use messages::message_comment_body::DataCommentBody;
use reddit_meme_analyzer::commons::{
    constants::queues::{QUEUE_COMMENTS_TO_JOIN, QUEUE_COMMENTS_TO_FILTER_STUDENTS},
    utils::{
        logger::{logger_create, Logger},
        middleware::{
            MiddlewareConnection,
            middleware_send_msg_end, MiddlewareService,
        },
    },
};

mod handlers;
mod messages;

struct WorkerFilterStudents;

impl MiddlewareService<Vec<DataCommentBody>> for WorkerFilterStudents {
    fn process_message(
        &self,
        payload: &mut Vec<DataCommentBody>,
        n_processed: &mut usize,
        exchange: &Exchange,
        logger: &Logger
    ) {
        handle_comments(payload, n_processed, &logger, &exchange);
    }

    fn process_end(&self, exchange: &Exchange) {
        middleware_send_msg_end(exchange, QUEUE_COMMENTS_TO_JOIN);
    }
}

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);
    {
        let consumer = middleware.get_consumer(QUEUE_COMMENTS_TO_FILTER_STUDENTS);
        let exchange = middleware.get_direct_exchange();

        let worker = WorkerFilterStudents;

        worker.consume(&consumer, &exchange, &logger);
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
