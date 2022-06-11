use amiquip::{Exchange};

use crate::{
    utils::{logger::Logger, middleware::middleware_send_msg}, constants::queues::{AVG_TO_FILTER_SCORE, QUEUE_TO_CLIENT}, messages::outbound::message_client::Data,
};

fn publish_score_avg(exchange: &Exchange, score_avg: f32) {
    middleware_send_msg(exchange, &score_avg, AVG_TO_FILTER_SCORE);

    let payload = Data{
        key: "posts_score__avg".to_string(),
        value: score_avg.to_string()
    };
    middleware_send_msg(exchange, &payload, QUEUE_TO_CLIENT)
}

pub fn handle_calc_avg_end(exchange: &Exchange, logger: &Logger, score_sum: u64, score_count: usize) {
    logger.info("doing end".to_string());
    publish_score_avg(&exchange, score_sum as f32 / score_count as f32);
}