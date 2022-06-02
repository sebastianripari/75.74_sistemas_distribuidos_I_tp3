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

const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

const LOG_LEVEL: &str = "debug";

fn handle_post(payload: String, exchange: &Exchange, n_post_received: &mut usize, logger: Logger) {
    let posts = Post::deserialize_multiple(payload);
    let posts_clone = posts.clone();

    *n_post_received = *n_post_received + posts.len();

    let scores: Value;
    let posts_: Value;

    scores = posts
        .into_iter()
        .map(|post| {
            json!({
                "score": post.score
            })
        })
        .rev()
        .collect();

    posts_ = posts_clone
        .into_iter()
        .map(|post| {
            json!({
                "post_id": post.id,
                "score": post.score,
                "url": post.url,
            })
        })
        .rev()
        .collect();

    if let Err(err) = exchange.publish(Publish::new(
        scores.to_string().as_bytes(),
        QUEUE_POSTS_TO_AVG,
    )) {
        logger.info(format!("could not publish: {:?}", err))
    }

    if let Err(err) = exchange.publish(Publish::new(
        posts_.to_string().as_bytes(),
        QUEUE_POSTS_TO_FILTER_SCORE,
    )) {
        logger.info(format!("could not publish: {:?}", err))
    }

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
                "body": comment.body
            })
        })
        .rev()
        .collect();

    if let Err(err) = exchange.publish(Publish::new(
        comments_.to_string().as_bytes(),
        QUEUE_COMMENTS_TO_FILTER_STUDENTS,
    )) {
        logger.info(format!("could not publish: {:?}", err))
    }

    if *n_comment_received % 100000 == 0 {
        logger.info(format!("n comment received: {}", n_comment_received))
    }
}

fn handle_comment_end(exchange: &Exchange, comments_done: &mut bool, logger: Logger) {
    *comments_done = true;
    logger.info("comments done".to_string());
}

fn handle_post_end(exchange: &Exchange, posts_done: &mut bool, logger: Logger) {
    *posts_done = true;

    exchange
        .publish(Publish::new(
            "end".to_string().as_bytes(),
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

    loop {
        let mut n_post_received: usize = 0;
        let mut n_comment_received: usize = 0;

        let mut posts_done = false;
        let mut comments_done = false;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    let splited: Vec<&str> = body.split('|').collect();

                    let opcode = splited[0].parse::<u8>().unwrap();
                    let payload = splited[1..].join("|");

                    match opcode {
                        OPCODE_POST_END => {
                            handle_post_end(&exchange, &mut posts_done, logger.clone());
                        }
                        OPCODE_COMMENT_END => {
                            handle_comment_end(&exchange, &mut comments_done, logger.clone());
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

                    if posts_done && comments_done {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
}
