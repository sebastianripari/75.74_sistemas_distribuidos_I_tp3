use serde::Serialize;

#[derive(Serialize)]
pub struct Data {
    pub key: String,
    pub value: String
}