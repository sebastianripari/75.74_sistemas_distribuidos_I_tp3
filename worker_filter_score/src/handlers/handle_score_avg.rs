use amiquip::{Exchange, Publish};

use crate::constants::queues::QUEUE_POSTS_TO_JOIN;
use crate::messages::opcodes::{MESSAGE_OPCODE_NORMAL};
use crate::messages::outbound::message_posts::{PostData, MessagePosts};
use crate::utils::middleware::{middleware_send_msg_end, middleware_send_msg};
use crate::{entities::post::Post, utils::logger::Logger};

fn send_posts_filtered(exchange: &Exchange, posts: &mut Vec<Post>) {
    for chunk in posts.chunks(100) {
        let payload_posts: Vec<PostData> = chunk
            .iter()
            .map(|post| {
                PostData {
                    post_id: post.id.to_string(),
                    url: post.url.to_string()
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
    posts: &mut Vec<Post>,
    exchange: &Exchange,
) {
    logger.info(format!("received score_avg: {}", score_avg));
    logger.info("start filtering posts".to_string());
    posts.retain(|post| (post.score as f32) > score_avg);

    send_posts_filtered(exchange, posts);
    send_posts_filtered_end(exchange);

    logger.info("finish filtering posts".to_string());
}
