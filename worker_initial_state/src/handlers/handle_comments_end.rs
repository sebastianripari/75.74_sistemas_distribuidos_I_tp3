use amiquip::{Exchange, Publish};

use crate::{QUEUE_COMMENTS_TO_MAP, messages::{message_comments::MessageComments, opcodes::MESSAGE_OPCODE_END}, utils::logger::Logger};

pub fn handle_comments_end(exchange: &Exchange, logger: Logger) {
    let msg_end = MessageComments {
        opcode: MESSAGE_OPCODE_END,
        payload: None
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_MAP,
        ))
        .unwrap();

    logger.info("comments done".to_string());
}