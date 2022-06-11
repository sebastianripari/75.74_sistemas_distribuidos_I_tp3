use amiquip::{Exchange};

use crate::{
    utils::{middleware::{middleware_end_reached, middleware_consumer_end}}, constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS}
};

pub fn handle_end(exchange: &Exchange, n_end: &mut usize) -> bool {
    let mut end = false;

    if middleware_end_reached(n_end) {

        middleware_consumer_end(
            &exchange, 
            QUEUE_COMMENTS_TO_FILTER_STUDENTS
        );

        middleware_consumer_end(
            &exchange, 
            QUEUE_COMMENTS_TO_FILTER_STUDENTS
        );

        end = true;
    }

    return end
}
