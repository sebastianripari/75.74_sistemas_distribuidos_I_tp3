use serde::Serialize;

#[derive(Serialize)]
pub struct Data {
    pub post_id: String,
}

#[derive(Serialize)]
pub struct MessageComment {
    pub opcode: u8,
    pub payload: Option<Data>
}