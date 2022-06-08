use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Data {
    pub key: String,
    pub value: String
}

#[derive(Deserialize)]
pub struct Message {
    pub opcode: u8,
    pub payload: Option<Data>
}