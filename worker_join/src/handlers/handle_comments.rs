use std::collections::HashMap;
use amiquip::{Publish, Exchange};
use crate::{
    messages::{
        inbound::message_comments::Data,
        opcodes::MESSAGE_OPCODE_NORMAL,
        outbound::message_client::{DataOutbound, MessageClient},
    },
    utils::logger::{Logger, LOG_RATE}, constants::queues::QUEUE_TO_CLIENT,
};

pub fn handle_comments(
    payload: Data,
    n: &mut usize,
    n_joins: &mut usize,
    posts: &mut HashMap<String, String>,
    logger: &Logger,
    exchange: &Exchange
) {
    *n += 1;

    if let Some(post_url) = posts.get(&payload.post_id) {
        logger.debug(format!(
            "join ok, id: {}, url: {}",
            payload.post_id, post_url
        ));

        let msg = MessageClient {
            opcode: MESSAGE_OPCODE_NORMAL,
            payload: Some(DataOutbound {
                key: "best_students_memes_url".to_string(),
                value: post_url.to_string(),
            }),
        };

        exchange
            .publish(Publish::new(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                QUEUE_TO_CLIENT,
            ))
            .unwrap();

        *n_joins += 1;
        if *n_joins % 100 == 0 {
            logger.info(format!("n joins: {}", n_joins));
        }
    }

    posts.remove(&payload.post_id);

    if *n % LOG_RATE == 0 {
        logger.info(format!("n comments processed: {}", n));
    }
}
