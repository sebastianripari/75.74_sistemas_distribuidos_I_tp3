use amiquip::{Exchange, Publish};

use crate::{
    messages::{
        inbound::message_comments::CommentInboundData,
        opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::{
            message_comments_body::{DataCommentBody, MessageOutboundCommentsBody},
            message_comments_sentiment::{DataCommentSentiment, MessageOutboundCommentsSentiment},
        },
    },
    utils::logger::{Logger, LOG_RATE},
    constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY}
};
use regex::Regex;

const COMMENT_PERMALINK_REGEX: &str = r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

fn publish_comments_body(payload: &Vec<CommentInboundData>, exchange: &Exchange) {
    let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

    let payload_comments_body: Vec<DataCommentBody> = payload
        .iter()
        .map(|comment| {
            let permalink = comment.permalink.to_string();

            let post_id = regex.captures(&permalink).unwrap().get(1).unwrap().as_str();

            DataCommentBody {
                post_id: post_id.to_string(),
                body: comment.body.to_string(),
            }
        })
        .rev()
        .collect();

    let msg_comments = MessageOutboundCommentsBody {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload_comments_body),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comments).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
        ))
        .unwrap();
}

fn publish_comments_sentiment(payload: &Vec<CommentInboundData>, exchange: &Exchange) {
    let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

    let payload_comments_sentiment: Vec<DataCommentSentiment> = payload
        .iter()
        .map(|comment| {
            let permalink = comment.permalink.to_string();

            let post_id = regex.captures(&permalink).unwrap().get(1).unwrap().as_str();

            DataCommentSentiment {
                post_id: post_id.to_string(),
                sentiment: comment.sentiment,
            }
        })
        .rev()
        .collect();

    let msg_comments = MessageOutboundCommentsSentiment {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload_comments_sentiment),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comments).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_GROUP_BY,
        ))
        .unwrap();
}

pub fn handle_comments(
    payload: &mut Vec<CommentInboundData>,
    n: &mut usize,
    exchange: &Exchange,
    logger: &Logger,
) {
    *n += payload.len();

    let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

    payload.retain(|comment| match regex.captures(&comment.permalink) {
        Some(_) => true,
        None => false,
    });

    publish_comments_body(&payload, exchange);
    publish_comments_sentiment(&payload, exchange);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n));
    }
}
