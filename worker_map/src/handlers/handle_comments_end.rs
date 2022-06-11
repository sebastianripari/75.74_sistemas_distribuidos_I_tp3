use amiquip::{Exchange};

use crate::{
    utils::{middleware::{middleware_end_reached, middleware_consumer_end, Message}}, constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY}, messages::{outbound::{data_comments_body::{DataCommentBody, VecDataCommentBody}, data_comments_sentiment::{DataCommentSentiment, VecDataCommentSentiment}}, opcodes::MESSAGE_OPCODE_END}
};

pub fn handle_end(exchange: &Exchange, n_end: &mut usize) -> bool {
    let mut end = false;

    if middleware_end_reached(n_end) {
        println!("doing end");

        middleware_consumer_end(
            &exchange,
            QUEUE_COMMENTS_TO_FILTER_STUDENTS
        );

        middleware_consumer_end(
            &exchange,
            QUEUE_COMMENTS_TO_GROUP_BY
        );

        end = true;
    }

    return end
}
