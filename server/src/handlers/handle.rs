use crate::{messages::inbound::message::Data, utils::{socket::SocketWriter}};

pub fn handle(payload: Data, client: &mut SocketWriter) {
    let key = payload.key;
    let value = payload.value;

    client.send(format!("{}: {}", key, value))
}