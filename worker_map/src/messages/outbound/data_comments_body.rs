use serde::Serialize;

#[derive(Serialize)]
pub struct DataCommentBody {
    pub post_id: String,
    pub body: String
}

pub type VecDataCommentBody = Vec<DataCommentBody>;