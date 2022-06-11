use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataCommentBodySentiment {
    pub permalink: String,
    pub body: String,
    pub sentiment: f32
}

pub type VecDataCommentBodySentiment = Vec<DataCommentBodySentiment>;