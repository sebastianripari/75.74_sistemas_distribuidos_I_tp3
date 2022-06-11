use amiquip::Exchange;

use crate::{
    constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY},
    utils::middleware::middleware_consumer_end,
};

pub fn handle_end(exchange: &Exchange, n_end: &mut usize) -> bool {
    let end = middleware_consumer_end(
        n_end,
        &exchange,
        [
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
            QUEUE_COMMENTS_TO_GROUP_BY,
        ].to_vec()
    );

    return end;
}
