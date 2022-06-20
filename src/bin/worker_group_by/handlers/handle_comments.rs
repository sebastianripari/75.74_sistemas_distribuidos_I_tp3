use std::collections::HashMap;

use reddit_meme_analyzer::commons::utils::logger::Logger;

use crate::messages::data_comment_sentiment::DataCommentSentiment;

pub fn handle_comments(
    payload: Vec<DataCommentSentiment>,
    n: &mut usize,
    logger: &Logger,
    posts: &HashMap<String, String>,
    comments: &mut HashMap<String, (usize, f32, String)>,
) {
    *n += payload.len();

    if payload.len() == 0 {
        return
    }

    for comment in payload {
        logger.debug(format!("processing: {}", comment.post_id));
        if let Some(url) = posts.get(&comment.post_id) {
            if url != "" {
                if let Some((count, sum, _)) = comments.get_mut(&comment.post_id) {
                    *count = *count + 1;
                    *sum = *sum + comment.sentiment;
                } else {
                    comments.insert(comment.post_id, (1, comment.sentiment, url.to_string()));
                }
            }
        }
    }

    if *n % 10000 < 10 {
        logger.info(format!("n comments processed: {}", n))
    }
}
