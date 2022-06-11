use serde::Serialize;

#[derive(Serialize)]
pub struct DataPostUrl {
    pub key: String,
    pub value: String
}