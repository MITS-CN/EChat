use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outgoing {
    Resp { ok: bool, msg: String },
    Event { source: String, content: String },
    Data { kind: String, data: serde_json::Value },
    Close { reason: String },
    AuthRequired,
}

impl Outgoing {
    pub fn ok(msg: impl Into<String>) -> Self {
        Outgoing::Resp { ok: true, msg: msg.into() }
    }
    pub fn err(msg: impl Into<String>) -> Self {
        Outgoing::Resp { ok: false, msg: msg.into() }
    }
    pub fn event(source: impl Into<String>, content: impl Into<String>) -> Self {
        Outgoing::Event { source: source.into(), content: content.into() }
    }
    pub fn data(kind: impl Into<String>, data: serde_json::Value) -> Self {
        Outgoing::Data { kind: kind.into(), data }
    }
    pub fn close(reason: impl Into<String>) -> Self {
        Outgoing::Close { reason: reason.into() }
    }
    pub fn auth_required() -> Self {
        Outgoing::AuthRequired
    }
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"type":"resp","ok":false,"msg":"serialize error"}"#.into()
        })
    }
}