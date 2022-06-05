use serde::Serialize;

#[derive(Serialize)]
pub struct DataCommentSentiment {
    pub post_id: String,
    pub sentiment: f32
}

#[derive(Serialize)]
pub struct MessageOutboundCommentsSentiment {
    pub opcode: u8,
    pub payload: Option<Vec<DataCommentSentiment>>
}