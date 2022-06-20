use amiquip::Exchange;
use reddit_meme_analyzer::commons::{constants::queues::QUEUE_POSTS_TO_JOIN, utils::{middleware::{middleware_send_msg, middleware_send_msg_end}, logger::Logger}};

use crate::{PostTuple, messages::data_post_url::PostData};

fn send_posts_filtered(exchange: &Exchange, posts: &mut Vec<PostTuple>) {
    for chunk in posts.chunks(100) {
        let payload_posts: Vec<PostData> = chunk
            .iter()
            .map(|post| {
                PostData {
                    post_id: post.0.clone(),
                    url: post.2.clone()
                }
            })
            .rev()
            .collect();

        middleware_send_msg(exchange, &payload_posts, QUEUE_POSTS_TO_JOIN);
    }
}

fn send_posts_filtered_end(exchange: &Exchange) {
    middleware_send_msg_end(exchange, QUEUE_POSTS_TO_JOIN)
}

pub fn handle_score_avg(
    score_avg: f32,
    logger: &Logger,
    posts: &mut Vec<PostTuple>,
    exchange: &Exchange,
) {
    logger.info(format!("received score_avg: {}", score_avg));
    logger.info("start filtering posts".to_string());
    posts.retain(|post| (post.1 as f32) > score_avg);

    send_posts_filtered(exchange, posts);
    send_posts_filtered_end(exchange);

    logger.info("finish filtering posts".to_string());
}
