use amiquip::{
    ConsumerMessage, ConsumerOptions, Exchange, QueueDeclareOptions,
};
use constants::queues::QUEUE_COMMENTS_TO_MAP;
use messages::inbound::message_comments::MessageInboundComments;
use messages::opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL};
use utils::logger::logger_create;
use utils::rabbitmq::rabbitmq_connect;
use std::env;
use std::{thread, time::Duration};
use handlers::handle_comments_end::handle_comments_end;
use handlers::handle_comments::handle_comments;

mod messages;
mod utils;
mod handlers;
mod constants;

fn main() {
    let logger = logger_create();

    logger.info("start".to_string());

    let mut n_producers = 1;
    if let Ok(value) = env::var("N_PRODUCERS") {
        n_producers = value.parse::<usize>().unwrap();
    }

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(QUEUE_COMMENTS_TO_MAP, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    let mut n_processed = 0;
    let mut ends = 0;
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: MessageInboundComments = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        ends += 1;
                        logger.info(format!("current ends: {}, ends to reach: {}", ends, n_producers));
                        if ends == n_producers {
                            handle_comments_end(&exchange, &logger);
                        }
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_comments(
                            &mut payload.unwrap(),
                            &mut n_processed,
                            &exchange,
                            &logger
                        );
                    }
                    _ => {
                    }
                }

                consumer.ack(delivery).unwrap();

                if ends == n_producers {
                    break;
                }
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }

    logger.info("worker map shutdown".to_string());
}
