use amiquip::{Exchange, Publish};

use crate::{
    entities::post::Post,
    messages::opcodes::MESSAGE_OPCODE_NORMAL,
    messages::outbound::{
        message_posts::{MessagePosts, PostData},
        message_scores::MessageScores,
    },
    utils::logger::Logger,
    LOG_RATE, QUEUE_POSTS_TO_AVG, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY,
};

fn publish_scores(exchange: &Exchange, posts: &Vec<Post>) {
    let payload_scores = posts.iter().map(|post| post.score).rev().collect();

    let msg_scores = MessageScores {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload_scores),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_scores).unwrap().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();
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

    let msg_posts = MessagePosts {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload_posts),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_posts).unwrap().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();

        exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_posts).unwrap().as_bytes(),
            QUEUE_POSTS_TO_GROUP_BY,
        ))
        .unwrap();
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
