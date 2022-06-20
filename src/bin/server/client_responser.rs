use std::sync::mpsc::Receiver;

use amiquip::ConsumerMessage;
use reddit_meme_analyzer::commons::{utils::{socket::SocketWriter, logger::Logger, middleware::{middleware_connect, middleware_create_channel, middleware_declare_queue, middleware_create_consumer, Message, MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}}, constants::queues::QUEUE_TO_CLIENT};

use crate::{handlers::handle::{self, handle}, messages::message::Data};

pub fn client_responser(logger: &Logger, clients: Receiver<SocketWriter>) {
    let mut connection = middleware_connect(&logger);
    let channel = middleware_create_channel(&mut connection);
    let queue = middleware_declare_queue(&channel, QUEUE_TO_CLIENT);
    let consumer = middleware_create_consumer(&queue);

    if let Ok(mut client) = clients.recv() {
        let mut best_students_memes_url_handled = false;
        let mut posts_score_avg_handled = false;
        let mut meme_with_best_sentiment_handled = false;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
                    let msg: Message<Data> = serde_json::from_str(&body).unwrap();
                    let opcode = msg.opcode;
                    let payload = msg.payload;

                    match opcode {
                        MESSAGE_OPCODE_END => {
                            break;
                        }
                        MESSAGE_OPCODE_NORMAL => {
                            handle(
                                payload.unwrap(),
                                &mut client,
                                logger,
                                &mut best_students_memes_url_handled,
                                &mut posts_score_avg_handled,
                                &mut meme_with_best_sentiment_handled,
                            );
                        }
                        _ => {}
                    }

                    consumer.ack(delivery).unwrap();

                    if best_students_memes_url_handled
                        && posts_score_avg_handled
                        && meme_with_best_sentiment_handled
                    {
                        break;
                    }
                }
                _ => {
                    break;
                }
            }
        }
    }

    if connection.close().is_ok() {
        logger.info("[client_responser]: connection closed".to_string());
    }
}
