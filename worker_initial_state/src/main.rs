use crate::{entities::comment::Comment, entities::post::Post, utils::logger::Logger};
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use messages::{
    message_comments::{CommentData, MessageCommentss},
    message_posts::{MessagePosts, PostData},
    message_scores::MessageScores,
};
use serde_json;
use std::{env, thread, time::Duration};

mod entities;
mod messages;
mod utils;

// queue input
const QUEUE_INITIAL_STATE: &str = "QUEUE_INITIAL_STATE";

// queue output
const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";
const QUEUE_POSTS_TO_FILTER_SCORE: &str = "QUEUE_POSTS_TO_FILTER_SCORE";
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// msg opcodes
const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

const LOG_LEVEL: &str = "debug";
const LOG_RATE: usize = 100000;

fn publish_scores(exchange: &Exchange, posts: &Vec<Post>) {
    let payload_scores = posts.iter().map(|post| post.score).rev().collect();

    let msg_scores = MessageScores {
        opcode: 1,
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
        opcode: 1,
        payload: Some(payload_posts),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_posts).unwrap().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();
}

fn handle_post(payload: String, exchange: &Exchange, n_post_received: &mut usize, logger: Logger) {
    let posts = Post::deserialize_multiple(payload);

    publish_scores(&exchange, &posts);
    publish_posts(&exchange, &posts);

    *n_post_received = *n_post_received + posts.len();

    if *n_post_received % LOG_RATE == 0 {
        logger.info(format!("n post received: {}", n_post_received))
    }
}

fn publish_comments(exchange: &Exchange, comments: &Vec<Comment>) {
    let payload_comments: Vec<CommentData> = comments
        .into_iter()
        .map(|comment| CommentData {
            permalink: comment.permalink.clone(),
            body: comment.body.clone(),
            sentiment: comment.sentiment,
        })
        .rev()
        .collect();

    let msg_comments = MessageCommentss {
        opcode: 1,
        payload: Some(payload_comments),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_comments).unwrap().as_bytes(),
            QUEUE_COMMENTS_TO_MAP,
        ))
        .unwrap();
}

fn handle_comment(
    payload: String,
    exchange: &Exchange,
    n_comment_received: &mut usize,
    logger: Logger,
) {
    let comments = Comment::deserialize_multiple(payload);

    publish_comments(exchange, &comments);

    *n_comment_received = *n_comment_received + comments.len();

    if *n_comment_received % LOG_RATE == 0 {
        logger.info(format!("n comment received: {}", n_comment_received))
    }
}

fn handle_comment_end(exchange: &Exchange, logger: Logger) {
    logger.info("comments done".to_string());

    exchange
        .publish(Publish::new(
            "end".to_string().as_bytes(),
            QUEUE_COMMENTS_TO_MAP,
        ))
        .unwrap()
}

fn publish_end_scores(exchange: &Exchange) {
    let msg_end = MessageScores {
        opcode: 0,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();
}

fn publish_end_posts(exchange: &Exchange) {
    let msg_end = MessagePosts {
        opcode: 0,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_end).unwrap().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();
}

fn handle_post_end(exchange: &Exchange, logger: Logger) {
    publish_end_scores(exchange);
    publish_end_posts(exchange);

    logger.info("posts done".to_string());
}

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger.info("start".to_string());

    let rabbitmq_user;
    match env::var("RABBITMQ_USER") {
        Ok(value) => rabbitmq_user = value,
        Err(_) => {
            panic!("could not get rabbitmq user from env")
        }
    }

    let rabbitmq_password;
    match env::var("RABBITMQ_PASSWORD") {
        Ok(value) => rabbitmq_password = value,
        Err(_) => {
            panic!("could not get rabbitmq password from env")
        }
    }

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection;
    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            println!("connected with rabbitmq");
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);

    let queue = channel
        .queue_declare(QUEUE_INITIAL_STATE, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    let mut n_post_received: usize = 0;
    let mut n_comment_received: usize = 0;

    let mut posts_end = false;
    let mut comments_end = false;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                let splited: Vec<&str> = body.split('|').collect();

                let opcode = splited[0].parse::<u8>().unwrap();
                let payload = splited[1..].join("|");

                match opcode {
                    OPCODE_POST_END => {
                        handle_post_end(&exchange, logger.clone());
                        posts_end = true;
                    }
                    OPCODE_COMMENT_END => {
                        handle_comment_end(&exchange, logger.clone());
                        comments_end = true;
                    }
                    OPCODE_POST => {
                        handle_post(
                            payload.to_string(),
                            &exchange,
                            &mut n_post_received,
                            logger.clone(),
                        );
                    }
                    OPCODE_COMMENT => {
                        handle_comment(
                            payload.to_string(),
                            &exchange,
                            &mut n_comment_received,
                            logger.clone(),
                        );
                    }
                    _ => logger.info("opcode invalid".to_string()),
                }

                consumer.ack(delivery).unwrap();

                if posts_end && comments_end {
                    logger.info("doing end".to_string());
                    break;
                }
            }
            _ => {}
        }
    }
}
