use serde::Serialize;

#[derive(Serialize)]
pub struct MessageScoreAvg {
    pub opcode: u8,
    pub payload: Option<f32>
}