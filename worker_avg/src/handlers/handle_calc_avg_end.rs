use amiquip::{Exchange};

use crate::{
    utils::{logger::Logger, middleware::{middleware_send_msg, middleware_send_msg_end}}, constants::queues::{AVG_TO_FILTER_SCORE, QUEUE_TO_CLIENT}, messages::outbound::message_client::Data,
};

fn publish_score_avg(exchange: &Exchange, score_avg: f32) {
    middleware_send_msg(exchange, &score_avg, AVG_TO_FILTER_SCORE);
    middleware_send_msg_end(exchange, AVG_TO_FILTER_SCORE);

    middleware_send_msg(exchange, &Data{
        key: "posts_score_avg".to_string(),
        value: score_avg.to_string()
    }, QUEUE_TO_CLIENT);
}

pub fn handle_calc_avg_end(exchange: &Exchange, logger: &Logger, score_sum: u64, score_count: usize) {
    logger.info("doing end".to_string());
    publish_score_avg(&exchange, score_sum as f32 / score_count as f32);
}