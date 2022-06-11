use amiquip::{Exchange};

use crate::{
    messages::{
        opcodes::MESSAGE_OPCODE_END, outbound::{message_comments_body::MessageOutboundCommentsBody, message_comments_sentiment::MessageOutboundCommentsSentiment},
    },
    utils::{middleware::{middleware_end_reached, middleware_consumer_end}}, constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS}
};

pub fn handle_end(exchange: &Exchange, n_end: &mut usize) -> bool {
    let mut end = false;

    if middleware_end_reached(n_end) {

        middleware_consumer_end::<MessageOutboundCommentsBody>(
            &exchange, 
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
            MessageOutboundCommentsBody{
                opcode: MESSAGE_OPCODE_END,
                payload: None,
            }
        );

        middleware_consumer_end::<MessageOutboundCommentsSentiment>(
            &exchange, 
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
            MessageOutboundCommentsSentiment{
                opcode: MESSAGE_OPCODE_END,
                payload: None,
            }
        );

        end = true;
    }

    return end
}
