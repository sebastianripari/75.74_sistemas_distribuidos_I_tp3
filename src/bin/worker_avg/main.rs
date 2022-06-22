use amiquip::ConsumerMessage;
use handlers::handle_calc_avg::handle_calc_avg;
use messages::message::Data;
use reddit_meme_analyzer::commons::{
    constants::queues::{AVG_TO_FILTER_SCORE, QUEUE_POSTS_TO_AVG, QUEUE_TO_CLIENT},
    utils::{
        logger::logger_create,
        middleware::{
            middleware_end_reached, middleware_send_msg, middleware_send_msg_end, Message,
            MiddlewareConnection, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL,
        },
    },
};

mod handlers;
mod messages;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let middleware = MiddlewareConnection::new(&logger);

    {
        let consumer = middleware.get_consumer(QUEUE_POSTS_TO_AVG);
        let exchange = middleware.get_direct_exchange();

        let mut n_processed: usize = 0;
        let mut end = false;
        let mut n_end = 0;
        let mut score_count: usize = 0;
        let mut score_sum: u64 = 0;

        for message in consumer.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<Vec<i32>> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        let score_avg: f32 = score_sum as f32 / score_count as f32;

                        if middleware_end_reached(&mut n_end, 0) {
                            middleware_send_msg(
                                &exchange,
                                &Data {
                                    key: "posts_score_avg".to_string(),
                                    value: score_avg.to_string(),
                                },
                                QUEUE_TO_CLIENT,
                            );

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
        }
    }

    middleware.close();

    logger.info("shutdown".to_string());
}
