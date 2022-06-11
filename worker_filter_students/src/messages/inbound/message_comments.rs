use serde::Deserialize;

#[derive(Deserialize)]
pub struct DataInboundComment {
    pub post_id: String,
    pub body: String,
}

#[derive(Deserialize)]
pub struct MessageInboundComments {
    pub opcode: u8,
    pub payload: Option<Vec<DataInboundComment>>
}

