use amiquip::{Exchange, Publish};

use crate::{utils::logger::Logger, QUEUE_COMMENTS_TO_FILTER_STUDENTS, messages::{opcodes::MESSAGE_OPCODE_END, outbound::message_comments_body::MessageOutboundCommentsBody}};

pub fn publish_comments_body_end(exchange: &Exchange) {
    let msg_end = MessageOutboundCommentsBody {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
        ))
        .unwrap();
}

pub fn publish_comments_sentiment_end(exchange: &Exchange) {
    let msg_end = MessageOutboundCommentsBody {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
        ))
        .unwrap();
}

pub fn handle_comments_end(exchange: &Exchange, logger: &Logger) {
    logger.info("doing end".to_string());

    publish_comments_body_end(exchange);
    publish_comments_sentiment_end(exchange);
}
