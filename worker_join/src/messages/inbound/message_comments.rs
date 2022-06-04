use serde::Deserialize;

#[derive(Deserialize)]
pub struct Data {
    pub post_id: String,
}

#[derive(Deserialize)]
pub struct MessageComments {
    pub opcode: u8,
    pub payload: Option<Data>
}