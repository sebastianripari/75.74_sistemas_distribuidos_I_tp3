use amiquip::{Exchange, Publish};

use crate::{
    utils::logger::Logger,
    messages::{
        opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::message_score_avg::MessageScoreAvg,
        outbound::message_client::{MessageClient, Data},
    }, constants::queues::{AVG_TO_FILTER_SCORE, QUEUE_TO_CLIENT}
};

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

    let msg_client = MessageClient {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(Data{
            key: "posts_score_avg".to_string(),
            value: score_avg.to_string()
        })
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&msg_client).unwrap().as_bytes(),
            QUEUE_TO_CLIENT,
        ))
        .unwrap();
}

pub fn handle_calc_avg_end(exchange: &Exchange, logger: &Logger, score_sum: u64, score_count: usize) {
    logger.info("doing end".to_string());
    publish_score_avg(&exchange, score_sum as f32 / score_count as f32);
}