use amiquip::{Exchange, Publish};

use crate::{
    messages::{
        inbound::message_comments::CommentInboundData,
        opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::message_comments::{CommentOutboundData, MessageOutboundComments},
    },
    utils::logger::Logger,
    LOG_RATE, QUEUE_COMMENTS_TO_FILTER_STUDENTS,
};
use regex::Regex;

fn publish_comments(payload: Vec<CommentOutboundData>, exchange: &Exchange) {
    let msg_comments = MessageOutboundComments {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comments).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
        ))
        .unwrap();
}

pub fn handle_comments(
    payload: Vec<CommentInboundData>,
    n: &mut usize,
    exchange: &Exchange,
    logger: &Logger,
) {
    *n += payload.len();

    let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();
    const COMMENT_PERMALINK_REGEX: &str =
        r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

    let payload_comments: Vec<CommentOutboundData> = payload
        .iter()
        .map(|comment| {
            let permalink = comment.permalink.to_string();

            if let Some(captures) = regex.captures(&permalink) {
                let post_id = captures.get(1).unwrap().as_str();

                CommentOutboundData {
                    post_id: post_id.to_string(),
                    body: comment.body.to_string(),
                }
            } else {
                CommentOutboundData {
                    post_id: "".to_string(),
                    body: comment.body.to_string(),
                }
            }
        })
        .rev()
        .collect();

    publish_comments(payload_comments, exchange);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n));
    }
}
