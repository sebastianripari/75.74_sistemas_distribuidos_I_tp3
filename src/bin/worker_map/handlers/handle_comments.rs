use amiquip::Exchange;
use reddit_meme_analyzer::commons::{utils::{middleware::middleware_send_msg, logger::{Logger, LOG_RATE}}, constants::queues::{QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY}};
use regex::Regex;

use crate::messages::{data_comment_body_sentiment::DataCommentBodySentiment, data_comment_body::DataCommentBody, data_comment_sentiment::DataCommentSentiment};

const COMMENT_PERMALINK_REGEX: &str = r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

fn send_comments_body(payload: &[DataCommentBodySentiment], exchange: &Exchange) {
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

    middleware_send_msg(
        exchange,
        &payload_comments_body,
        QUEUE_COMMENTS_TO_FILTER_STUDENTS,
    )
}

fn send_comments_sentiment(payload: &[DataCommentBodySentiment], exchange: &Exchange) {
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

    middleware_send_msg(
        exchange,
        &payload_comments_sentiment,
        QUEUE_COMMENTS_TO_GROUP_BY,
    )
}

pub fn handle_comments(
    payload: &mut Vec<DataCommentBodySentiment>,
    n: &mut usize,
    exchange: &Exchange,
    logger: &Logger,
) {
    *n += payload.len();

    let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();

    payload.retain(|comment| regex.captures(&comment.permalink).is_some());

    send_comments_body(payload, exchange);
    send_comments_sentiment(payload, exchange);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n));
    }
}