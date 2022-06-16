use amiquip::ConsumerMessage;
use constants::queues::{QUEUE_POSTS_TO_AVG, AVG_TO_FILTER_SCORE, QUEUE_TO_CLIENT};
use handlers::handle_calc_avg::handle_calc_avg;
use messages::outbound::message_client::Data;
use utils::{
    logger::logger_create,
    middleware::{
        middleware_connect, middleware_create_channel, middleware_create_consumer,
        middleware_create_exchange, middleware_declare_queue, Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL, middleware_consumer_end, middleware_send_msg, middleware_end_reached, middleware_send_msg_end,
    },
};

mod constants;
mod handlers;
mod messages;
mod utils;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue = middleware_declare_queue(&channel, QUEUE_POSTS_TO_AVG);
    let consumer = middleware_create_consumer(&queue);
    let exchange = middleware_create_exchange(&channel);

    let mut n_processed: usize = 0;
    let mut end = false;
    let mut n_end = 0;

    let mut score_count: usize = 0;
    let mut score_sum: u64 = 0;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<i32>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        let score_avg: f32 = score_sum as f32 / score_count as f32;

                        if middleware_end_reached(&mut n_end) {
                            middleware_send_msg(&exchange, &Data{
                                key: "posts_score_avg".to_string(),
                                value: score_avg.to_string()
                            }, QUEUE_TO_CLIENT);

                            middleware_send_msg(&exchange, &score_avg, AVG_TO_FILTER_SCORE);
                            middleware_send_msg_end(&exchange, AVG_TO_FILTER_SCORE);
                            end = true;
                        }
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        handle_calc_avg(
                            payload.unwrap(),
                            &mut n_processed,
                            &logger,
                            &mut score_count,
                            &mut score_sum,
                        );
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
            _ => {
                break;
            }
        }
    }

    if connection.close().is_ok() {
        logger.info("connection closed".to_string());
    }

    logger.info("shutdown".to_string());
}
