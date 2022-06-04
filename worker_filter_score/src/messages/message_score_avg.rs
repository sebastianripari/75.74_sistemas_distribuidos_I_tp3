use serde::Deserialize;

#[derive(Deserialize)]
pub struct MessageScoreAvg {
    pub opcode: u8,
    pub payload: Option<i32>
}