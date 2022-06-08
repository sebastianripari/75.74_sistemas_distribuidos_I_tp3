use std::sync::mpsc::Receiver;

use amiquip::{QueueDeclareOptions, ConsumerOptions, ConsumerMessage};
use crate::{utils::{socket::{SocketWriter}}, messages::{inbound::message::Message, opcodes::{MESSAGE_OPCODE_END, MESSAGE_OPCODE_NORMAL}}, handlers::handle::{self, handle}};
use crate::{utils::{logger::Logger, rabbitmq::rabbitmq_connect}, constants::queues::QUEUE_TO_CLIENT};

pub fn client_responser(logger: &Logger, clients: Receiver<SocketWriter>) {
    let mut rabbitmq_connection = rabbitmq_connect(&logger);
    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(QUEUE_TO_CLIENT, QueueDeclareOptions::default())
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();

    loop {
        if let Ok(mut client) = clients.recv() {

            let mut end = false;

            for message in consumer.receiver().iter() {
                match message {
                    ConsumerMessage::Delivery(delivery) => {
                        let body = String::from_utf8_lossy(&delivery.body);
                        let msg: Message = serde_json::from_str(&body).unwrap();
                        let opcode = msg.opcode;
                        let payload = msg.payload;
        
                        match opcode {
                            MESSAGE_OPCODE_END => {
                                end = true;
                            }
                            MESSAGE_OPCODE_NORMAL => {
                                handle(
                                    payload.unwrap(),
                                    &mut client,
                                    logger,
                                );
                            }
                            _ => {}
                        }
        
                        consumer.ack(delivery).unwrap();
        
                        if end {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}