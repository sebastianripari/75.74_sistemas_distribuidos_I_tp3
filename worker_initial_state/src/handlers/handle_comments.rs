use amiquip::{Exchange, Publish};

use crate::{
    entities::comment::Comment,
    messages::{
        opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::message_comments::{CommentData, MessageComments},
    },
    utils::logger::Logger,
    LOG_RATE, QUEUE_COMMENTS_TO_MAP,
};

fn publish_comments(exchange: &Exchange, comments: &Vec<Comment>) {
    let payload_comments: Vec<CommentData> = comments
        .into_iter()
        .map(|comment| CommentData {
            permalink: comment.permalink.clone(),
            body: comment.body.clone(),
            sentiment: comment.sentiment,
        })
        .rev()
        .collect();

    let msg_comments = MessageComments {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload_comments),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comments).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_MAP,
        ))
        .unwrap();
}

pub fn handle_comments(
    payload: String,
    exchange: &Exchange,
    n_comment_received: &mut usize,
    logger: Logger,
) {
    let comments = Comment::deserialize_multiple(payload);

    publish_comments(exchange, &comments);

    *n_comment_received = *n_comment_received + comments.len();

    if *n_comment_received % LOG_RATE == 0 {
        logger.info(format!("n comment received: {}", n_comment_received))
    }
}
