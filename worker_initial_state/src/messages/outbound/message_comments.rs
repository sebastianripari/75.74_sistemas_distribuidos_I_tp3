use serde::Serialize;

#[derive(Serialize)]
pub struct CommentData {
    pub permalink: String,
    pub body: String,
    pub sentiment: f32
}

#[derive(Serialize)]
pub struct MessageComments {
    pub opcode: u8,
    pub payload: Option<Vec<CommentData>>
}