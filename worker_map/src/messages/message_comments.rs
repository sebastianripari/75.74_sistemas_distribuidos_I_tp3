use serde::Deserialize;

#[derive(Deserialize)]
pub struct CommentData {
    pub permalink: String,
    pub body: String,
    pub sentiment: f32
}

#[derive(Deserialize)]
pub struct MessageComments {
    pub opcode: u8,
    pub payload: Option<Vec<CommentData>>
}