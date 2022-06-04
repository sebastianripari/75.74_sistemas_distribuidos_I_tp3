use serde::Serialize;

#[derive(Serialize)]
pub struct MessageScores {
    pub opcode: u8,
    pub payload: Option<Vec<i32>>
}