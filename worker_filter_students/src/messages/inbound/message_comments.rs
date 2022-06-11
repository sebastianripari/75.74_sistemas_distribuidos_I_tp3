use serde::Deserialize;

#[derive(Deserialize)]
pub struct DataInboundComment {
    pub post_id: String,
    pub body: String,
}

#[derive(Deserialize)]
pub struct MessageInboundComments {
    pub opcode: u8,
    //#[serde(default = "default_resource")]
    pub payload: Option<Vec<DataInboundComment>>
}

//fn default_resource() -> Option<Vec<DataInboundComment>> {
//    return Some(Vec::new())
//}

