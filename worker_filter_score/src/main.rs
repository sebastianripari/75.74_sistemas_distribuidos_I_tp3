use std::{thread, time::Duration, env};
use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Publish, Exchange};
use serde::{Deserialize};
use serde_json::{json, Value};

mod entities;
mod utils;

use crate::{entities::post::Post, utils::logger::Logger};

#[derive(Deserialize, Debug)]
struct MsgPost {
    post_id: String,
    score: i32,
    url: String,
}

#[derive(Deserialize, Debug)]
struct MsgScoreAvg {
    score_avg: i32
}

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

    let mut stop = false;

    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
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

    let queue_posts = channel.queue_declare(QUEUE_POSTS_TO_FILTER_SCORE, QueueDeclareOptions::default()).unwrap();
    let queue_score_avg = channel.queue_declare(AVG_TO_FILTER_SCORE, QueueDeclareOptions::default()).unwrap();

    let consumer_posts = queue_posts.consume(ConsumerOptions::default()).unwrap();
    let consumer_score_avg = queue_score_avg.consume(ConsumerOptions::default()).unwrap();
    
    loop {
        let mut posts = Vec::new();

        if stop {
            break;
        }

        let mut score_avg = 0;

        for message in consumer_posts.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    if body == "stop" {
                        stop = true;
                        break;
                    }

                    if body == "end" {
                        break;
                    }

                    let value: MsgPost = serde_json::from_str(&body).unwrap();
                    if posts.len() % 10000 == 0 {
                        logger.debug(format!("processing: {:?}", value));
                    }
                    let post_id = value.post_id.to_string();
                    let score = value.score;
                    let url = value.url;

                    let post = Post::new(post_id, score, url);
                    posts.push(post);
                    
                    consumer_posts.ack(delivery).unwrap();
                }
                _ => {}
            }
        }

        for message in consumer_score_avg.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    if body == "stop" {
                        stop = true;
                        break;
                    }

                    let value: MsgScoreAvg = serde_json::from_str(&body).unwrap();

                    score_avg = value.score_avg;
                    logger.info(format!("received score_avg: {}", score_avg));

                    consumer_score_avg.ack(delivery).unwrap();
                    break;
                }
                _ => {}
            }
        }

        logger.info("start filtering posts".to_string());
        let mut n_post_published = 0;
        for post in posts {
            if post.score > score_avg {
                exchange.publish(Publish::new(
                    json!({
                        "post_id": post.id.to_string(),
                        "url": post.url.to_string()
                    }).to_string().as_bytes(),
                    QUEUE_POSTS_TO_JOIN
                )).unwrap();
                n_post_published = n_post_published + 1;
                if n_post_published % 10000 == 0 {
                    logger.debug(format!("publish post id: {}, url: {}", post.id, post.url));
                }
            }
        }
        logger.info("finish filtering posts".to_string());
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }
    
    logger.info("shutdown".to_string());
}
