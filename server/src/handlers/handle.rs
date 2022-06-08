use crate::{messages::inbound::message::Data, utils::{socket::SocketWriter, logger::Logger}};

pub fn handle(payload: Data, client: &mut SocketWriter, logger: &Logger) {
    logger.info(format!("new msg to send a client: {:?}", payload));

    let key = payload.key;
    let value = payload.value;

    client.send(format!("{}:{}", key, value))
}