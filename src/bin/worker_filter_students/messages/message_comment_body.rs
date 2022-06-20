use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataCommentBody {
    pub post_id: String,
    pub body: String,
}