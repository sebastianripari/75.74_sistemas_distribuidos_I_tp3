use serde::Deserialize;

#[derive(Deserialize)]
pub struct DataInboundPost {
    pub post_id: String,
    pub url: String,
}

#[derive(Deserialize)]
pub struct MessageInboundPosts {
    pub opcode: u8,
    pub payload: Option<Vec<DataInboundPost>>
}