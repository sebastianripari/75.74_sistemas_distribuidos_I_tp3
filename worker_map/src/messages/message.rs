use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Message<T> {
    pub opcode: u8,
    pub payload: Option<T>
}
