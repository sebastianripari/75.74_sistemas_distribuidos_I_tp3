use amiquip::Exchange;
use reddit_meme_analyzer::commons::{utils::{logger::{Logger, LOG_RATE}, middleware::middleware_send_msg}, constants::queues::QUEUE_COMMENTS_TO_JOIN};

use crate::messages::{message_comment::DataComment, message_comment_body::DataCommentBody};


const STUDENTS_WORDS: [&'static str; 5] =
["university", "college", "student", "teacher", "professor"];

pub fn handle_comments(
payload: Vec<DataCommentBody>,
n: &mut usize,
logger: &Logger,
exchange: &Exchange,
) {
if payload.len() == 0 {
    return
}

*n += payload.len();

for comment in payload {
    logger.debug(format!("processing: {}", comment.post_id));
    for word in STUDENTS_WORDS {
        if comment.body.to_ascii_lowercase().contains(word) {
            logger.debug("match student".to_string());

            let payload = DataComment {
                post_id: comment.post_id.to_string(),
            };
            middleware_send_msg(exchange, &payload, QUEUE_COMMENTS_TO_JOIN);
            break;
        }
    }
}

if *n % LOG_RATE < 10 {
    logger.info(format!("n processed: {}", n))
}
}
