use amiquip::{ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions};
use constants::queues::{QUEUE_POSTS_TO_FILTER_SCORE, AVG_TO_FILTER_SCORE};
use handlers::handle_posts::handle_posts;
use handlers::handle_score_avg::handle_score_avg;
use messages::{
    inbound::message_posts::MessagePosts,
    inbound::message_score_avg::MessageScoreAvg,
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL},
};
use utils::{rabbitmq::rabbitmq_connect, logger::logger_create};
use std::{thread, time::Duration};

mod entities;
mod handlers;
mod messages;
mod utils;
mod constants;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
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

    let mut end = false;

    let mut n_processed = 0;
    let mut posts = Vec::new();

    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessagePosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(payload.unwrap(), &mut n_processed, &logger, &mut posts);
                    }
                    _ => {}
                }

                consumer_posts.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    end = false;
    for message in consumer_score_avg.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageScoreAvg = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_NORMAL => {
                        handle_score_avg(payload.unwrap(), &logger, &mut posts, &exchange);
                        end = true;
                    }
                    _ => {}
                }

                consumer_score_avg.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
