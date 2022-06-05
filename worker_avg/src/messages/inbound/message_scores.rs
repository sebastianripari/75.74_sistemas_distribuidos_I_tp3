use serde::Deserialize;

#[derive(Deserialize)]
pub struct MessageScores {
    pub opcode: u8,
    pub payload: Option<Vec<i32>>
}