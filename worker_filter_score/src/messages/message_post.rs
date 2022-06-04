use serde::Deserialize;

#[derive(Deserialize)]
pub struct Post {
    pub post_id: String,
    pub score: i32,
    pub url: String
}

#[derive(Deserialize)]
pub struct MessagePost {
    pub opcode: u8,
    pub payload: Option<Vec<Post>>
}