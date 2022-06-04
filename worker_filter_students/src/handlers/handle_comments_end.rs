use amiquip::{Exchange, Publish};

use crate::{utils::logger::Logger, messages::{outbound::message_comment::MessageOutboundComment, opcodes::MESSAGE_OPCODE_END}, QUEUE_COMMENTS_TO_JOIN};

pub fn handle_comments_end(exchange: &Exchange, logger: &Logger) {
    logger.info("doing end".to_string());

    let msg_end = MessageOutboundComment {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_JOIN,
        ))
        .unwrap();
}
