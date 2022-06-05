use amiquip::{Exchange, Publish};

use crate::{messages::outbound::message_comments::MessageOutboundComments, QUEUE_COMMENTS_TO_FILTER_STUDENTS, utils::logger::Logger};

pub fn publish_comments_end(exchange: &Exchange) {
    let msg_end = MessageOutboundComments {
        opcode: 0,
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

    publish_comments_end(exchange);
}
