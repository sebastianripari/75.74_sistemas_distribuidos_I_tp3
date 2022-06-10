use std::sync::mpsc::Receiver;

use crate::{
    constants::queues::QUEUE_TO_CLIENT,
    utils::{logger::Logger, rabbitmq::rabbitmq_connect},
};
use crate::{
    handlers::handle::handle,
    messages::{inbound::message::Message, opcodes::MESSAGE_OPCODE_NORMAL},
    utils::socket::SocketWriter,
};
use amiquip::{ConsumerMessage, ConsumerOptions, QueueDeclareOptions};

pub fn client_responser(logger: &Logger, clients: Receiver<SocketWriter>) {
    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(QUEUE_TO_CLIENT, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    if let Ok(mut client) = clients.recv() {

        let mut best_students_memes_url_handled = false;
        let mut posts_score_avg_handled = false;
        let mut meme_with_best_sentiment_handled = false;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
                    let msg: Message = serde_json::from_str(&body).unwrap();
                    let opcode = msg.opcode;
                    let payload = msg.payload;

                    match opcode {
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
                _ => {}
            }
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        logger.info("rabbitmq connection closed".to_string())
    }
}
