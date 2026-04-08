mod clock;
#[cfg(feature = "crypto")]
mod crypto;
mod db;
mod error;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
mod native_sync;
mod operation;
mod persistence;
mod query;
mod snapshot;
mod sync;
mod value;

#[cfg(target_arch = "wasm32")]
mod wasm;

pub use clock::{HybridClock, Revision, VersionMarker};
#[cfg(feature = "crypto")]
pub use crypto::{EncryptedPayload, Identity, PublicIdentity, SecretBoxKey, SignedPayload};
pub use db::{
    Chain, ChangeEvent, ChangeSubscription, MapEntry, Primadb, QueryBuilder, Subscription,
};
pub use error::{PrimadbError, Result};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
pub use native_sync::NativeWebSocketSync;
pub use operation::{Operation, OperationAction, OperationValue};
pub use query::{QueryDirection, QueryFilter, QueryOrder, QuerySpec};
pub use snapshot::DatabaseSnapshot;
pub use sync::{SyncEnvelope, SyncFrame};
pub use value::{FieldState, FieldValue, NodeId, NodeState, SetState};
