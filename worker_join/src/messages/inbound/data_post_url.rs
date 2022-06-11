use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataPostUrl {
    pub post_id: String,
    pub url: String
}
