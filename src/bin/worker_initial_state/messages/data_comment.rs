use serde::Serialize;

#[derive(Serialize)]
pub struct CommentData {
    pub permalink: String,
    pub body: String,
    pub sentiment: f32
}