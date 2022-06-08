use serde::Serialize;

#[derive(Serialize)]
pub struct DataOutbound {
    pub key: String,
    pub value: String
}

#[derive(Serialize)]
pub struct MessageClient {
    pub opcode: u8,
    pub payload: Option<DataOutbound>
}