use amiquip::{Exchange, Publish};

use crate::{
    messages::{
        inbound::message_comments::DataInboundComment, opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::message_comment::{MessageOutboundComment, DataOutboundComment},
    },
    utils::logger::Logger,
    LOG_RATE, QUEUE_COMMENTS_TO_JOIN,
};

const STUDENTS_WORDS: [&'static str; 5] =
    ["university", "college", "student", "teacher", "professor"];

fn publish_comments_filtered(exchange: &Exchange, post_id: String) {
    let payload = DataOutboundComment {
        post_id: post_id,
    };

    let msg_comment = MessageOutboundComment {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comment).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_JOIN,
        ))
        .unwrap();
}

pub fn handle_comments(
    payload: Vec<DataInboundComment>,
    n: &mut usize,
    logger: &Logger,
    exchange: &Exchange,
) {
    *n += payload.len();

    for comment in payload {
        logger.debug(format!("processing: {}", comment.post_id));
        for word in STUDENTS_WORDS {
            if comment.body.to_ascii_lowercase().contains(word) {
                logger.debug("match student".to_string());
                publish_comments_filtered(exchange, comment.post_id.to_string());
                break;
            }
        }
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n))
    }
}
