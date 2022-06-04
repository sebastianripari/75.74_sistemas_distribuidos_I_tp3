use amiquip::{Exchange, Publish};

use crate::{utils::logger::Logger, QUEUE_TO_CLIENT, AVG_TO_FILTER_SCORE, messages::{opcodes::MESSAGE_OPCODE_NORMAL, message_score_avg::MessageScoreAvg}};

fn publish_score_avg(exchange: &Exchange, score_avg: f32) {
    let msg = MessageScoreAvg {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(score_avg),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg).unwrap().as_bytes(),
            AVG_TO_FILTER_SCORE,
        ))
        .unwrap();

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg).unwrap().as_bytes(),
            QUEUE_TO_CLIENT,
        ))
        .unwrap();
}

pub fn handle_calc_avg_end(exchange: &Exchange, logger: &Logger, score_sum: u64, score_count: usize) {
    logger.info("doing end".to_string());
    publish_score_avg(&exchange, score_sum as f32 / score_count as f32);
}