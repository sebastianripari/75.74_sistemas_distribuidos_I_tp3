use serde::Serialize;

#[derive(Serialize)]
pub struct DataOutboundComment {
    pub post_id: String,
}

#[derive(Serialize)]
pub struct MessageOutboundComment {
    pub opcode: u8,
    pub payload: Option<DataOutboundComment>
}