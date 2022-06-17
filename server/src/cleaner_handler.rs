use std::sync::{mpsc::Receiver, Arc, RwLock};

use crate::{
    utils::{
        logger::Logger,
        middleware::{
            middleware_connect, middleware_create_channel, middleware_create_exchange,
            middleware_stop_all_consumers,
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
        // only case SIGTERM or SIGFAULT 
        middleware_stop_all_consumers(&exchange);
    }

    if connection.close().is_ok() {
        logger.info("[cleaner_handler]: connection closed".to_string())
    }
}
