use std::sync::{mpsc::Receiver, Arc, RwLock};

use crate::{
    constants::queues::{
        AVG_TO_FILTER_SCORE, QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY,
        QUEUE_COMMENTS_TO_JOIN, QUEUE_COMMENTS_TO_MAP, QUEUE_INITIAL_STATE, QUEUE_POSTS_TO_AVG,
        QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY, QUEUE_POSTS_TO_JOIN, QUEUE_TO_CLIENT,
    },
    utils::{
        logger::Logger,
        middleware::{
            middleware_connect, middleware_create_channel, middleware_create_exchange,
            middleware_send_msg_end,
        },
    },
};

pub fn cleaner_handler(
    receiver_signal: Receiver<&str>,
    running_lock: Arc<RwLock<bool>>,
    logger: Logger,
) {
    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let exchange = middleware_create_exchange(&channel);

    let end_type = receiver_signal.recv().unwrap();

    if let Ok(mut running) = running_lock.write() {
        *running = false;
    }

    if end_type == "end_sigterm" {
        // only case SIGTERM ro SIGFAULT

        for _ in 0..(2 * 6)  {
            // producer * consumer: N_WORKER_INITIAL_STATE * N_WORKER_MAP
            middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_MAP);
        }
    
        for _ in 0..(1 * 2) {
            // producer * consumer: server * N_WORKER_INITIAL_STATE
            middleware_send_msg_end(&exchange, QUEUE_INITIAL_STATE);
        }
    
        for _ in 0..(2 * 2) {
            // producer * producer: N_WORKER_INITIAL_STATE * N_WORKER_FILTER_STUDENTS
            middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_FILTER_STUDENTS);
        }
    
        for _ in 0..(2 * 1) {
            // producer * consumer: N_WORKER_INITIAL_STATE * N_WORKER_FILTER_SCORE
            middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_FILTER_SCORE);
        }
    
        for _ in 0..(1 * 1){
            // producer * consumer: N_WORKER_AVG * N_WORKER_FILTER_SCORE
            middleware_send_msg_end(&exchange, AVG_TO_FILTER_SCORE);
        }
        
        for _ in 0..(6 * 1) {
            // producer * consumer: N_WORKER_MAP * N_WORKER_GROUP_BY
            middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_GROUP_BY);
        }
       
        for _ in 0..(2 * 1) {
            // producer * consumer: N_WORKER_FILTER_STUDENTS * N_WORKER_JOIN
            middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_JOIN);
        }
        
        for _ in 0..(2 * 1) {
            // producer * consumer: N_WORKER_INITIAL_STATE * N_WORKER_AVG
            middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_AVG);
        }
    
        for _ in 0..(2 * 1) {
            // producer * consumer: N_WORKER_INITIAL_STATE * N_WORKER_GROUP_BY
            middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_GROUP_BY);
        }
    
        for _ in 0..(1 * 1) {
            // producer: N_WORKER_FILTER_SCORE * N_WORKER_JOIN
            middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_JOIN);
        }

        middleware_send_msg_end(&exchange, QUEUE_TO_CLIENT);

    }

    if connection.close().is_ok() {
        logger.info("[cleaner_handler]: connection closed".to_string())
    }
}
