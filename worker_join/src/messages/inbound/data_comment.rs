use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataComment {
    pub post_id: String,
}
