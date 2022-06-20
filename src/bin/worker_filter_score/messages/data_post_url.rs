use serde::Serialize;

#[derive(Serialize)]
pub struct PostData {
    pub post_id: String,
    pub url: String
}