use serde::Deserialize;

#[derive(Deserialize)]
pub struct DataInboundComment {
    pub post_id: String,
    pub sentiment: f32,
}

#[derive(Deserialize)]
pub struct MessageInboundComments {
    pub opcode: u8,
    #[serde(default)]
    pub payload: Option<Vec<DataInboundComment>>
}