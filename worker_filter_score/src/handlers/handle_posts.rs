use crate::{
    entities::post::Post, messages::message_posts::PostData, utils::logger::Logger, LOG_RATE,
};

pub fn handle_posts(payload: Vec<PostData>, n: &mut usize, logger: &Logger, posts: &mut Vec<Post>) {
    *n += payload.len();

    for post in payload {
        let post_id = post.post_id;
        let score = post.score;
        let url = post.url;

        let post = Post::new(post_id, score, url);
        posts.push(post);
    }

    if posts.len() % LOG_RATE == 0 {
        logger.info(format!("processing: {}", n));
    }
}
