use serde::Serialize;

#[derive(Serialize)]
pub struct Data {
    pub key: String,
    pub value: String
}

#[derive(Serialize)]
pub struct MessageClient {
    pub opcode: u8,
    pub payload: Option<Data>
}