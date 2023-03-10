use serde::{Deserialize, Serialize};

pub async fn sleep(secs: u64) {
    tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
}

pub async fn sleep_millis(secs: u64) {
    tokio::time::sleep(tokio::time::Duration::from_millis(secs)).await;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item {
    pub key: u64,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RpcMsgCallValue {
    pub method: String,
    pub params: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RpcMsgReplyValue {
    Success(Vec<u8>),
    // TODO Error msg must contain an error code according to rpc standard protocol
    Error(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RpcMsgValueType {
    Call(RpcMsgCallValue),
    Reply(RpcMsgReplyValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RpcMsg {
    pub id: u64,
    pub value: RpcMsgValueType,
}

impl RpcMsg {
    pub fn new_call(id: u64, value: RpcMsgCallValue) -> Self {
        Self {
            id,
            value: RpcMsgValueType::Call(value),
        }
    }

    pub fn new_reply<T: Serialize>(id: u64, value: T) -> Self {
        let value = bincode::serialize(&value).unwrap();
        let value = RpcMsgValueType::Reply(RpcMsgReplyValue::Success(value));
        Self { id, value }
    }

    pub fn new_error_reply(id: u64, error: &str) -> Self {
        let value = RpcMsgValueType::Reply(RpcMsgReplyValue::Error(error.into()));
        Self { id, value }
    }
}
