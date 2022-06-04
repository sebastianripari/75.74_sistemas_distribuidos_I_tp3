use amiquip::{Exchange, Publish};

use crate::{utils::logger::Logger, messages::{message_posts::MessagePosts, message_scores::MessageScores, opcodes::{MESSAGE_OPCODE_END}}, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_AVG};

fn publish_end_scores(exchange: &Exchange) {
    let msg_end = MessageScores {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();
}

fn publish_end_posts(exchange: &Exchange) {
    let msg_end = MessagePosts {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();
}

pub fn handle_post_end(exchange: &Exchange, logger: Logger) {
    publish_end_scores(exchange);
    publish_end_posts(exchange);

    logger.info("posts done".to_string());
}