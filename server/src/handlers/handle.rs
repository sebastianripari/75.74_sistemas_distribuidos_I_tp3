use std::io::Read;

use crate::{utils::{socket::SocketWriter, logger::Logger}, messages::inbound::message::{Data}};

pub fn handle(payload: Data, client: &mut SocketWriter, logger: &Logger) {

    let key = payload.key;

    if key == "best_students_memes_url" {
        let value = payload.value.to_string();
        client.send(key.clone());
        client.send(value);
    }

    if key == "posts_score_avg" {
        let value = payload.value.to_string();
        client.send(key.clone());
        client.send(value);
    }

    if key == "meme_with_best_sentiment" {
        let value = payload.value;
        logger.info(format!("url to download: {}", value));
        if let Ok(response) = reqwest::blocking::get(value) {
            logger.info("image downloaded".to_string());
            if let Ok(response_bytes) = response.bytes() {
                client.send(key.clone());
                client.send(response_bytes.len().to_string());
                client.send_bytes(&response_bytes);
            }
        };
    }
}