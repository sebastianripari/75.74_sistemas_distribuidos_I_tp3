use std::io::Read;

use crate::{
    messages::inbound::message::Data,
    utils::{logger::Logger, socket::SocketWriter},
};

pub fn handle(
    payload: Data,
    client: &mut SocketWriter,
    logger: &Logger,
    best_students_memes_url_handled: &mut bool,
    posts_score_avg_handled: &mut bool,
    meme_with_best_sentiment_handled: &mut bool
) {
    let key = payload.key;

    if key == "best_students_memes_url" {
        let value = payload.value.to_string();
        client.send(key.clone());
        client.send(value);
        *best_students_memes_url_handled = true;
    }

    if key == "posts_score_avg" {
        let value = payload.value.to_string();
        client.send(key.clone());
        client.send(value);
        *posts_score_avg_handled = true;
    }

    if key == "meme_with_best_sentiment" {
        let url = payload.value;
        logger.info(format!("url to download: {}", url));

        let url_clone = url.clone();
        let mut url_splited: Vec<&str> = url_clone.split('/').collect();
        let filename = url_splited.pop().unwrap();

        logger.info(format!("filename: {}", filename));
        if let Ok(response) = reqwest::blocking::get(url) {
            logger.info("image downloaded".to_string());
            if let Ok(response_bytes) = response.bytes() {
                client.send(key.clone());
                client.send(filename.to_string());
                client.send(response_bytes.len().to_string());
                client.send_bytes(&response_bytes);
            }
        };

        *meme_with_best_sentiment_handled = true;
    }
}
