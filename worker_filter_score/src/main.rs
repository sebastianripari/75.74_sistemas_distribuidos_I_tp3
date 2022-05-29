use std::{thread, time::Duration, env};
use serde_json::{Value, json};
use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Publish, Exchange};

mod entities;
mod utils;

use crate::{entities::post::Post, utils::logger::Logger};

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

                    let value: Value = serde_json::from_str(&body).unwrap();
                    logger.debug(format!("processing: {}", value));
                    let post_id = value["post_id"].to_string();
                    let score = value["score"].to_string().parse::<i32>().unwrap();
                    let url = value["url"].to_string();

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

                    let value: Value = serde_json::from_str(&body).unwrap();

                    score_avg = value["score_avg"].to_string().parse::<i32>().unwrap();
                    logger.info(format!("received score_avg: {}", score_avg));

                    consumer_score_avg.ack(delivery).unwrap();
                    break;
                }
                _ => {}
            }
        }

        logger.info("start filtering posts".to_string());
        for post in posts {
            if post.score > score_avg {
                exchange.publish(Publish::new(
                    json!({
                        "post_id": post.id,
                        "url": post.url
                    }).to_string().as_bytes(),
                    QUEUE_POSTS_TO_JOIN
                )).unwrap();
            }
        }
        logger.info("finish filtering posts".to_string());
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }
    
    logger.info("shutdown".to_string());
}
