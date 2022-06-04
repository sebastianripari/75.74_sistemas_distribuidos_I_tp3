use serde::Deserialize;

#[derive(Deserialize)]
pub struct PostData {
    pub post_id: String,
    pub url: String
}

#[derive(Deserialize)]
pub struct MessagePosts {
    pub opcode: u8,
    pub payload: Option<Vec<PostData>>
}