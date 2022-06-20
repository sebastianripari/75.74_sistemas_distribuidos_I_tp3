use serde::Serialize;

#[derive(Serialize)]
pub struct DataCommentSentiment {
    pub post_id: String,
    pub sentiment: f32
}