use amiquip::{ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions};
use constants::queues::{QUEUE_COMMENTS_TO_GROUP_BY, QUEUE_POSTS_TO_GROUP_BY, QUEUE_TO_CLIENT};
use handlers::{handle_comments::handle_comments, handle_posts::handle_posts};
use messages::{
    inbound::{message_comments::MessageInboundComments, message_posts::MessageInboundPosts},
    opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL},
    outbound::message_client::{Data, MessageClient},
};
use std::{collections::HashMap, fs::File, io::Read, thread, time::Duration};
use utils::{logger::logger_create, rabbitmq::rabbitmq_connect};

mod constants;
mod handlers;
mod messages;
mod utils;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let exchange = Exchange::direct(&channel);

    let queue_posts = channel
        .queue_declare(QUEUE_POSTS_TO_GROUP_BY, QueueDeclareOptions::default())
        .unwrap();
    let consumer_posts = queue_posts.consume(ConsumerOptions::default()).unwrap();

    let queue_comments = channel
        .queue_declare(QUEUE_COMMENTS_TO_GROUP_BY, QueueDeclareOptions::default())
        .unwrap();
    let consumer_comments = queue_comments.consume(ConsumerOptions::default()).unwrap();

    let mut end = false;
    let mut n_posts_processed = 0;
    let mut posts = HashMap::new();
    for message in consumer_posts.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundPosts = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_posts(
                            payload.unwrap(),
                            &mut n_posts_processed,
                            &logger,
                            &mut posts,
                        );
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
    logger.info("end consuming posts".to_string());

    let mut n_comments_processed = 0;
    let mut comments = HashMap::new();
    end = false;
    for message in consumer_comments.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        end = true;
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            payload.unwrap(),
                            &mut n_comments_processed,
                            &logger,
                            &posts,
                            &mut comments,
                        );
                    }
                    _ => {}
                }

                consumer_comments.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {}
        }
    }
    logger.info("end consuming comments".to_string());

    logger.info("finding max".to_string());
    let max = comments.iter().max_by(|a, b| {
        (a.1 .1 / (a.1 .0 as f32))
            .partial_cmp(&(b.1 .1 / (b.1 .0 as f32)))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let url = max.unwrap().1 .2.to_string();

    let msg = MessageClient {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(Data {
            key: "meme_with_best_sentiment".to_string(),
            value: url,
        }),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg).unwrap().as_bytes(),
            QUEUE_TO_CLIENT,
        ))
        .unwrap();

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("shutdown".to_string());
}
