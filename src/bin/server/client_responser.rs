use std::sync::mpsc::Receiver;

use amiquip::ConsumerMessage;
use reddit_meme_analyzer::commons::{
    constants::queues::QUEUE_TO_CLIENT,
    utils::{
        logger::Logger,
        middleware::{
            Message, MiddlewareConnection,
            MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL,
        },
        socket::SocketWriter,
    },
};

use crate::{handlers::handle::handle, messages::message::Data};

pub fn client_responser(logger: &Logger, clients: Receiver<SocketWriter>) {
    let middleware = MiddlewareConnection::new(&logger);
    {
        let consumer = middleware.get_consumer(QUEUE_TO_CLIENT);

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
    }

    middleware.close();
}
