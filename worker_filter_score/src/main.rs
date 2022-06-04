use crate::{entities::post::Post, utils::logger::Logger};
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use messages::{message_posts::MessagePosts, message_score_avg::MessageScoreAvg};
use serde_json::{json, Value};
use std::{env, thread, time::Duration};

mod entities;
mod messages;
mod utils;

const LOG_LEVEL: &str = "debug";

// queue input
const QUEUE_POSTS_TO_FILTER_SCORE: &str = "QUEUE_POSTS_TO_FILTER_SCORE";
const AVG_TO_FILTER_SCORE: &str = "AVG_TO_FILTER_SCORE";

// queue output
const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";

fn main() {
    let mut log_level = LOG_LEVEL.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }
    let logger = Logger::new(log_level);

    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

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
            panic!("could not get rabbitmq password user from env")
        }
    }

    let mut rabbitmq_connection;
    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            logger.info("connected with rabbitmq".to_string());
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);

    let queue_posts = channel
        .queue_declare(QUEUE_POSTS_TO_FILTER_SCORE, QueueDeclareOptions::default())
        .unwrap();
    let queue_score_avg = channel
        .queue_declare(AVG_TO_FILTER_SCORE, QueueDeclareOptions::default())
        .unwrap();

    let consumer_posts = queue_posts.consume(ConsumerOptions::default()).unwrap();
    let consumer_score_avg = queue_score_avg.consume(ConsumerOptions::default()).unwrap();

    let mut n_processed = 0;
    let mut posts = Vec::new();
    let mut score_avg = 0;

    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessagePosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                if opcode == 0 {
                    consumer_posts.ack(delivery).unwrap();
                    break;
                }

                if opcode == 1 {
                    if let Some(payload) = payload {
                        n_processed = n_processed + payload.len();

                        for post in payload {
                            let post_id = post.post_id;
                            let score = post.score;
                            let url = post.url;

                            let post = Post::new(post_id, score, url);
                            posts.push(post);
                        }

                        if posts.len() % 100000 == 0 {
                            logger.info(format!("processing: {}", n_processed));
                        }
                    }
                }

                consumer_posts.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    for message in consumer_score_avg.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageScoreAvg = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    1 => {
                        if let Some(payload) = payload {
                            score_avg = payload;
                            logger.info(format!("received score_avg: {}", score_avg));
                            consumer_score_avg.ack(delivery).unwrap();
                            break;
                        }
                    }
                    _ => {
                        consumer_score_avg.ack(delivery).unwrap();
                    }
                }
            }
            _ => {}
        }
    }

    logger.info("start filtering posts".to_string());
    posts.retain(|post| post.score > score_avg);

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
    exchange
        .publish(Publish::new(
            "end".to_string().as_bytes(),
            QUEUE_POSTS_TO_JOIN,
        ))
        .unwrap();
    logger.info("finish filtering posts".to_string());

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
