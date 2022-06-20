use amiquip::Exchange;
use reddit_meme_analyzer::commons::{utils::{middleware::middleware_send_msg, logger::{Logger, LOG_RATE}}, constants::queues::{QUEUE_POSTS_TO_AVG, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY}, entities::post::Post};

use crate::messages::data_post::PostData;

fn publish_scores(exchange: &Exchange, posts: &Vec<Post>) {
    let payload_scores: Vec<i32> = posts.iter().map(|post| post.score).rev().collect();
    middleware_send_msg(exchange, &payload_scores, QUEUE_POSTS_TO_AVG);
}

fn publish_posts(exchange: &Exchange, posts: &Vec<Post>) {
    let payload_posts: Vec<PostData> = posts
        .iter()
        .map(|post| PostData {
            post_id: post.id.clone(),
            score: post.score,
            url: post.url.clone(),
        })
        .rev()
        .collect();

    middleware_send_msg(exchange, &payload_posts, QUEUE_POSTS_TO_FILTER_SCORE);
    middleware_send_msg(exchange, &payload_posts, QUEUE_POSTS_TO_GROUP_BY);
}

pub fn handle_posts(
    payload: String,
    exchange: &Exchange,
    n_post_received: &mut usize,
    logger: Logger,
) {
    let posts = Post::deserialize_multiple(payload);

    publish_scores(&exchange, &posts);
    publish_posts(&exchange, &posts);

    *n_post_received = *n_post_received + posts.len();

    if *n_post_received % LOG_RATE == 0 {
        logger.info(format!("n post received: {}", n_post_received))
    }
}