use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DataPostUrl {
    pub post_id: String,
    pub url: String
}

#[derive(Serialize)]
pub struct DataPostUrlOutbound {
    pub key: String,
    pub value: String
}