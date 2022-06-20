use reddit_meme_analyzer::commons::{
    utils::logger::{Logger, LOG_RATE},
};

use crate::{messages::data_post_score_url::DataPostScoreUrl, PostTuple};

pub fn handle_posts(
    payload: Vec<DataPostScoreUrl>,
    n: &mut usize,
    logger: &Logger,
    posts: &mut Vec<PostTuple>,
) {
    *n += payload.len();

    for post in payload {
        let post_id = post.post_id;
        let score = post.score;
        let url = post.url;

        let post = (post_id, score, url);
        posts.push(post);
    }

    if posts.len() % LOG_RATE == 0 {
        logger.info(format!("processing: {}", n));
    }
}
