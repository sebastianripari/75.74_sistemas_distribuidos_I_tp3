use serde::Deserialize;

#[derive(Deserialize)]
pub struct CommentInboundData {
    pub permalink: String,
    pub body: String,
    pub sentiment: f32
}

#[derive(Deserialize)]
pub struct MessageInboundComments {
    pub opcode: u8,
    pub payload: Option<Vec<CommentInboundData>>
}