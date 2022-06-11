use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataCommentSentiment {
    pub post_id: String,
    pub sentiment: f32,
}