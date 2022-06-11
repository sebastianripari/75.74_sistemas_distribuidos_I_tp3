use serde::Serialize;

#[derive(Serialize)]
pub struct DataBestUrl {
    pub key: String,
    pub value: String
}