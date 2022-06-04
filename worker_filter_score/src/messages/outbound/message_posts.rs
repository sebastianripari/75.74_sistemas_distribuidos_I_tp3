use serde::Serialize;

#[derive(Serialize)]
pub struct PostData {
    pub post_id: String,
    pub url: String
}

#[derive(Serialize)]
pub struct MessagePosts {
    pub opcode: u8,
    pub payload: Option<Vec<PostData>>
}