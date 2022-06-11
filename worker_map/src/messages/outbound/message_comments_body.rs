use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct DataCommentBody {
    pub post_id: String,
    pub body: String
}

#[derive(Serialize, Clone)]
pub struct MessageOutboundCommentsBody {
    pub opcode: u8,
    pub payload: Option<Vec<DataCommentBody>>
}