use thiserror::Error;

#[derive(Debug, Error)]
pub enum PrimadbError {
    #[error("expected an object at `{path}`")]
    ExpectedObject { path: String },
    #[error("expected a field path at `{path}`")]
    ExpectedFieldPath { path: String },
    #[error("arrays of objects are not supported at `{path}`; use set() or {{$set: [...]}}")]
    ArrayOfObjectsUnsupported { path: String },
    #[error("sets may only contain plain objects or {{$link: \"node-id\"}} markers at `{path}`")]
    InvalidSetMember { path: String },
    #[error("set member references must be a node id, {{$link: \"node-id\"}}, or an object with `$id` at `{path}`")]
    InvalidMemberReference { path: String },
    #[error("path segment `{field}` on node `{node}` is a scalar and cannot be traversed")]
    TraversalIntoScalar { node: String, field: String },
    #[error("path segment `{field}` on node `{node}` is a set and cannot be traversed")]
    TraversalIntoSet { node: String, field: String },
    #[error("browser window is unavailable")]
    BrowserWindowUnavailable,
    #[error("browser storage is unavailable")]
    BrowserStorageUnavailable,
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("{0}")]
    Message(String),
}

pub type Result<T> = std::result::Result<T, PrimadbError>;
