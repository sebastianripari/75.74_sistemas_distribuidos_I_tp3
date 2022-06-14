use amiquip::{Exchange};

use crate::{
    entities::comment::Comment,
    messages::{
        outbound::message_comments::{CommentData},
    },
    utils::{logger::Logger, middleware::middleware_send_msg},
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

    middleware_send_msg(exchange, &payload_comments, QUEUE_COMMENTS_TO_MAP);
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
