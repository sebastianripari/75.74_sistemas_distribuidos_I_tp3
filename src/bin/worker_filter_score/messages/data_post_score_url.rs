use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataPostScoreUrl {
    pub post_id: String,
    pub score: i32,
    pub url: String
}