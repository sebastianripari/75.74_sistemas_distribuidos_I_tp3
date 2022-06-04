use amiquip::{Exchange, Publish};
use serde_json::{json, Value};

use crate::{entities::post::Post, utils::logger::Logger, QUEUE_POSTS_TO_JOIN};

fn publish_posts_filtered(exchange: &Exchange, posts: &mut Vec<Post>) {
    for chunk in posts.chunks(100) {
        let to_send: Value = chunk
            .into_iter()
            .map(|post| {
                json!({
                    "post_id": post.id.to_string(),
                    "url": post.url.to_string()
                })
            })
            .rev()
            .collect();

        exchange
            .publish(Publish::new(
                to_send.to_string().as_bytes(),
                QUEUE_POSTS_TO_JOIN,
            ))
            .unwrap();
    }
}

fn publish_posts_filtered_end(exchange: &Exchange) {
    exchange
        .publish(Publish::new(
            "end".to_string().as_bytes(),
            QUEUE_POSTS_TO_JOIN,
        ))
        .unwrap();
}

pub fn handle_score_avg(
    score_avg: f32,
    logger: &Logger,
    posts: &mut Vec<Post>,
    exchange: &Exchange,
) {
    logger.info(format!("received score_avg: {}", score_avg));
    logger.info("start filtering posts".to_string());
    posts.retain(|post| (post.score as f32) > score_avg);

    publish_posts_filtered(exchange, posts);
    publish_posts_filtered_end(exchange);

    logger.info("finish filtering posts".to_string());
}
