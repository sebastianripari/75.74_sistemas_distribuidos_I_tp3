use amiquip::{Exchange, Publish};

use crate::messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use crate::messages::outbound::message_posts::{PostData, MessagePosts};
use crate::{entities::post::Post, utils::logger::Logger, QUEUE_POSTS_TO_JOIN};

fn publish_posts_filtered(exchange: &Exchange, posts: &mut Vec<Post>) {
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

        let message_posts = MessagePosts{
            opcode: MESSAGE_OPCODE_NORMAL,
            payload: Some(payload_posts)
        };

        exchange
            .publish(Publish::new(
                serde_json::to_string(&message_posts).unwrap().as_bytes(),
                QUEUE_POSTS_TO_JOIN,
            ))
            .unwrap();
    }
}

fn publish_posts_filtered_end(exchange: &Exchange) {
    let msg_end = MessagePosts {
        opcode: MESSAGE_OPCODE_END,
        payload: None
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
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
