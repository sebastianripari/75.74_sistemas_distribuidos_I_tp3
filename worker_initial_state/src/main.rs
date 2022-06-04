use crate::{entities::comment::Comment, entities::post::Post, utils::logger::Logger};
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use serde_json::{json, Value};
use std::{env, thread, time::Duration};

mod entities;
mod utils;

// queue input
const QUEUE_INITIAL_STATE: &str = "QUEUE_INITIAL_STATE";

// queue output
const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";
const QUEUE_POSTS_TO_FILTER_SCORE: &str = "QUEUE_POSTS_TO_FILTER_SCORE";
const QUEUE_COMMENTS_TO_FILTER_STUDENTS: &str = "QUEUE_COMMENTS_TO_FILTER_STUDENTS";
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// msg opcodes
const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

const LOG_LEVEL: &str = "debug";

fn handle_post(payload: String, exchange: &Exchange, n_post_received: &mut usize, logger: Logger) {
    let posts = Post::deserialize_multiple(payload);

    *n_post_received = *n_post_received + posts.len();

    let payload_scores: Value;
    let payload_posts: Value;

    payload_scores = posts.iter().map(|post| post.score).rev().collect();

    payload_posts = posts
        .iter()
        .map(|post| {
            json!({
                "post_id": post.id,
                "score": post.score,
                "url": post.url,
            })
        })
        .rev()
        .collect();

    let msg_scores = json!({
        "opcode": 1,
        "payload": {
            "scores": payload_scores
        }
    });

    exchange
        .publish(Publish::new(
            msg_scores.to_string().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();

    exchange
        .publish(Publish::new(
            payload_posts.to_string().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();

    if *n_post_received % 100000 == 0 {
        logger.info(format!("n post received: {}", n_post_received))
    }
}

fn handle_comment(
    payload: String,
    exchange: &Exchange,
    n_comment_received: &mut usize,
    logger: Logger,
) {
    let comments = Comment::deserialize_multiple(payload);

    *n_comment_received = *n_comment_received + comments.len();

    let comments_: Value;

    comments_ = comments
        .into_iter()
        .map(|comment| {
            json!({
                "permalink": comment.permalink,
                "body": comment.body,
                "sentiment": comment.sentiment
            })
        })
        .rev()
        .collect();

    exchange
        .publish(Publish::new(
            comments_.to_string().as_bytes(),
            QUEUE_COMMENTS_TO_MAP,
        ))
        .unwrap();

    if *n_comment_received % 100000 == 0 {
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

fn handle_post_end(exchange: &Exchange, logger: Logger) {
    let msg_end = json!({
        "opcode": 0
    });

    exchange
        .publish(Publish::new(
            msg_end.to_string().as_bytes(),
            QUEUE_POSTS_TO_AVG,
        ))
        .unwrap();

    exchange
        .publish(Publish::new(
            "end".to_string().as_bytes(),
            QUEUE_POSTS_TO_FILTER_SCORE,
        ))
        .unwrap();

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
