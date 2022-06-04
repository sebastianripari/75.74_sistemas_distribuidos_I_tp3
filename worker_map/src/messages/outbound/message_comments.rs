use serde::Serialize;

#[derive(Serialize)]
pub struct CommentOutboundData {
    pub post_id: String,
    pub body: String
}

#[derive(Serialize)]
pub struct MessageOutboundComments {
    pub opcode: u8,
    pub payload: Option<Vec<CommentOutboundData>>
}