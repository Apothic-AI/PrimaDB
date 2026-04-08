use crate::clock::now_millis;
use crate::crypto::{EncryptedPayload, Identity, PublicIdentity, SecretBoxKey};
use crate::error::{PrimadbError, Result};
use crate::{DatabaseSnapshot, Operation, SyncFrame};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserGrant {
    pub root: String,
    #[serde(default = "default_true")]
    pub read: bool,
    #[serde(default = "default_true")]
    pub write: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRecord {
    pub alias: String,
    pub public_key: String,
    #[serde(default)]
    pub grants: Vec<UserGrant>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct LocalUser {
    pub alias: String,
    pub identity: Identity,
    pub grants: Vec<UserGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthClaims {
    pub alias: String,
    pub replica_id: String,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default = "now_millis")]
    pub issued_at_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthenticatedSyncFrame {
    pub claims: AuthClaims,
    pub signer: String,
    pub signature: String,
    pub frame: SyncFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EncryptedSyncFrame {
    pub claims: AuthClaims,
    pub signer: String,
    pub signature: String,
    pub payload: EncryptedPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecureSyncFrame {
    Plain(SyncFrame),
    Authenticated(AuthenticatedSyncFrame),
    Encrypted(EncryptedSyncFrame),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoredSnapshot {
    Plain(DatabaseSnapshot),
    Encrypted {
        alias: Option<String>,
        payload: EncryptedPayload,
    },
}

#[derive(Debug, Clone, Default)]
pub struct SecurityState {
    pub require_signed_sync: bool,
    pub trusted_users: BTreeMap<String, UserRecord>,
    pub local_user: Option<LocalUser>,
    pub transport_encryption_key: Option<SecretBoxKey>,
    pub snapshot_encryption_key: Option<SecretBoxKey>,
}

impl UserGrant {
    pub fn write_root(root: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            read: true,
            write: true,
        }
    }

    pub fn matches_root(&self, candidate: &str) -> bool {
        self.root == "*" || self.root == candidate
    }
}

impl UserRecord {
    pub fn new(alias: impl Into<String>, public_identity: &PublicIdentity) -> Self {
        Self {
            alias: alias.into(),
            public_key: public_identity.to_base64(),
            grants: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_grants(mut self, grants: Vec<UserGrant>) -> Self {
        self.grants = grants;
        self
    }

    pub fn public_identity(&self) -> Result<PublicIdentity> {
        PublicIdentity::from_base64(&self.public_key)
    }

    pub fn can_write_roots<'a>(&self, roots: impl IntoIterator<Item = &'a str>) -> bool {
        roots.into_iter()
            .all(|root| self.grants.iter().any(|grant| grant.write && grant.matches_root(root)))
    }
}

impl LocalUser {
    pub fn new(alias: impl Into<String>, identity: Identity, grants: Vec<UserGrant>) -> Self {
        Self {
            alias: alias.into(),
            identity,
            grants,
        }
    }

    pub fn public_key(&self) -> String {
        self.identity.public_key_base64()
    }
}

impl SecurityState {
    pub fn register_user(
        &mut self,
        alias: impl Into<String>,
        public_identity: &PublicIdentity,
        grants: Vec<UserGrant>,
    ) {
        let alias = alias.into();
        self.trusted_users
            .insert(alias.clone(), UserRecord::new(alias, public_identity).with_grants(grants));
    }

    pub fn set_local_user(
        &mut self,
        alias: impl Into<String>,
        identity: Identity,
        grants: Vec<UserGrant>,
    ) -> Result<()> {
        let alias = alias.into();
        let public_key = identity.public_key_base64();
        if let Some(existing) = self.trusted_users.get(&alias) {
            if existing.public_key != public_key {
                return Err(PrimadbError::Crypto(format!(
                    "alias `{alias}` does not match the configured public key"
                )));
            }
        } else {
            self.register_user(alias.clone(), &identity.public_identity(), grants.clone());
        }
        self.local_user = Some(LocalUser::new(alias, identity, grants));
        Ok(())
    }

    pub fn set_snapshot_encryption_key(&mut self, key: SecretBoxKey) {
        self.snapshot_encryption_key = Some(key);
    }

    pub fn set_transport_encryption_key(&mut self, key: SecretBoxKey) {
        self.transport_encryption_key = Some(key);
    }

    pub fn clear_local_user(&mut self) {
        self.local_user = None;
    }

    pub fn encode_sync_frame(
        &self,
        replica_id: &str,
        roots: Vec<String>,
        frame: SyncFrame,
    ) -> Result<SecureSyncFrame> {
        let Some(local_user) = &self.local_user else {
            if self.require_signed_sync {
                return Err(PrimadbError::Crypto(
                    "signed sync is required but no local user is configured".to_owned(),
                ));
            }
            return Ok(SecureSyncFrame::Plain(frame));
        };

        let claims = AuthClaims {
            alias: local_user.alias.clone(),
            replica_id: replica_id.to_owned(),
            roots,
            issued_at_millis: now_millis(),
        };
        let signer = local_user.public_key();
        if let Some(key) = &self.transport_encryption_key {
            let payload = key.encrypt_json(&frame)?;
            let signature = local_user
                .identity
                .sign_payload((claims.clone(), payload.clone()))?
                .signature;
            Ok(SecureSyncFrame::Encrypted(EncryptedSyncFrame {
                claims,
                signer,
                signature,
                payload,
            }))
        } else {
            let signature = local_user
                .identity
                .sign_payload((claims.clone(), frame.clone()))?
                .signature;
            Ok(SecureSyncFrame::Authenticated(AuthenticatedSyncFrame {
                claims,
                signer,
                signature,
                frame,
            }))
        }
    }

    pub fn decode_sync_frame(&self, frame: SecureSyncFrame) -> Result<SyncFrame> {
        match frame {
            SecureSyncFrame::Plain(frame) => {
                if self.require_signed_sync {
                    Err(PrimadbError::Crypto(
                        "received unsigned sync frame while signed sync is required".to_owned(),
                    ))
                } else {
                    Ok(frame)
                }
            }
            SecureSyncFrame::Authenticated(frame) => {
                let user = self.verify_claims(&frame.claims, &frame.signer)?;
                user.public_identity()?.verify_payload(&crate::SignedPayload {
                    signer: frame.signer,
                    signature: frame.signature,
                    payload: (frame.claims.clone(), frame.frame.clone()),
                })?;
                authorize_roots(user, &frame.claims, roots_for_frame(&frame.frame))?;
                Ok(frame.frame)
            }
            SecureSyncFrame::Encrypted(frame) => {
                let user = self.verify_claims(&frame.claims, &frame.signer)?;
                user.public_identity()?.verify_payload(&crate::SignedPayload {
                    signer: frame.signer,
                    signature: frame.signature,
                    payload: (frame.claims.clone(), frame.payload.clone()),
                })?;
                let key = self.transport_encryption_key.as_ref().ok_or_else(|| {
                    PrimadbError::Crypto(
                        "received encrypted sync frame but no transport key is configured"
                            .to_owned(),
                    )
                })?;
                let decoded: SyncFrame = key.decrypt_json(&frame.payload)?;
                authorize_roots(user, &frame.claims, roots_for_frame(&decoded))?;
                Ok(decoded)
            }
        }
    }

    pub fn encode_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<StoredSnapshot> {
        if let Some(key) = &self.snapshot_encryption_key {
            let payload = key.encrypt_json(&snapshot)?;
            Ok(StoredSnapshot::Encrypted {
                alias: self.local_user.as_ref().map(|user| user.alias.clone()),
                payload,
            })
        } else {
            Ok(StoredSnapshot::Plain(snapshot))
        }
    }

    pub fn decode_snapshot(&self, stored: StoredSnapshot) -> Result<DatabaseSnapshot> {
        match stored {
            StoredSnapshot::Plain(snapshot) => Ok(snapshot),
            StoredSnapshot::Encrypted { payload, .. } => {
                let key = self.snapshot_encryption_key.as_ref().ok_or_else(|| {
                    PrimadbError::Crypto(
                        "received encrypted snapshot but no snapshot key is configured"
                            .to_owned(),
                    )
                })?;
                key.decrypt_json(&payload)
            }
        }
    }

    fn verify_claims<'a>(&'a self, claims: &AuthClaims, signer: &str) -> Result<&'a UserRecord> {
        let user = self.trusted_users.get(&claims.alias).ok_or_else(|| {
            PrimadbError::Crypto(format!("unknown user alias `{}`", claims.alias))
        })?;
        if user.public_key != signer {
            return Err(PrimadbError::Crypto(format!(
                "signer mismatch for alias `{}`",
                claims.alias
            )));
        }
        Ok(user)
    }
}

pub fn roots_for_frame(frame: &SyncFrame) -> Vec<String> {
    match frame {
        SyncFrame::Sync { ops, .. } => roots_for_ops(ops),
        SyncFrame::Ack { .. } => Vec::new(),
    }
}

pub fn roots_for_ops(ops: &[Operation]) -> Vec<String> {
    let mut roots = Vec::new();
    for root in ops.iter().flat_map(operation_roots) {
        if !roots.iter().any(|candidate| candidate == &root) {
            roots.push(root);
        }
    }
    roots
}

pub fn operation_roots(op: &Operation) -> Vec<String> {
    match &op.action {
        crate::OperationAction::SetField { node, .. }
        | crate::OperationAction::AddSetMember { node, .. }
        | crate::OperationAction::RemoveSetMember { node, .. }
        | crate::OperationAction::DeleteField { node, .. } => vec![root_for_node(node)],
    }
}

pub fn root_for_node(node: &str) -> String {
    let first = node
        .split('/')
        .next()
        .unwrap_or(node)
        .split('~')
        .next()
        .unwrap_or(node);
    first.to_owned()
}

fn authorize_roots(user: &UserRecord, claims: &AuthClaims, actual_roots: Vec<String>) -> Result<()> {
    if !claims
        .roots
        .iter()
        .all(|claimed| actual_roots.iter().any(|actual| actual == claimed))
        && !actual_roots.is_empty()
    {
        return Err(PrimadbError::Crypto(format!(
            "frame for alias `{}` advertised roots {:?} but carried {:?}",
            claims.alias, claims.roots, actual_roots
        )));
    }

    if !actual_roots.is_empty() && !user.can_write_roots(actual_roots.iter().map(String::as_str)) {
        return Err(PrimadbError::Crypto(format!(
            "alias `{}` is not authorized for roots {:?}",
            claims.alias, actual_roots
        )));
    }

    Ok(())
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{SecurityState, SecureSyncFrame, UserGrant};
    use crate::{Identity, Operation, OperationAction, OperationValue, Revision, SyncFrame};

    #[test]
    fn signed_frames_round_trip() {
        let identity = Identity::generate();
        let mut state = SecurityState::default();
        state
            .set_local_user("alice", identity.clone(), vec![UserGrant::write_root("docs")])
            .unwrap();
        state.register_user(
            "alice",
            &identity.public_identity(),
            vec![UserGrant::write_root("docs")],
        );

        let encoded = state
            .encode_sync_frame(
                "replica-a",
                vec!["docs".to_owned()],
                SyncFrame::Ack {
                    from: "replica-a".to_owned(),
                    message_id: "m1".to_owned(),
                    applied: 1,
                },
            )
            .unwrap();

        match encoded {
            SecureSyncFrame::Authenticated(_) => {}
            other => panic!("expected authenticated frame, got {other:?}"),
        }
        state.decode_sync_frame(encoded).unwrap();
    }

    #[test]
    fn unauthorized_roots_are_rejected() {
        let identity = Identity::generate();
        let mut sender = SecurityState::default();
        sender
            .set_local_user("alice", identity.clone(), vec![UserGrant::write_root("docs")])
            .unwrap();
        sender.register_user(
            "alice",
            &identity.public_identity(),
            vec![UserGrant::write_root("docs")],
        );

        let mut receiver = SecurityState::default();
        receiver.require_signed_sync = true;
        receiver.register_user(
            "alice",
            &identity.public_identity(),
            vec![UserGrant::write_root("users")],
        );

        let secure = sender
            .encode_sync_frame(
                "replica-a",
                vec!["docs".to_owned()],
                SyncFrame::Sync {
                    from: "replica-a".to_owned(),
                    message_id: "m1".to_owned(),
                    ops: vec![Operation {
                        op_id: "op-1".to_owned(),
                        author: "replica-a".to_owned(),
                        revision: Revision {
                            millis: 1,
                            counter: 0,
                            actor: "replica-a".to_owned(),
                        },
                        action: OperationAction::SetField {
                            node: "docs".to_owned(),
                            field: "title".to_owned(),
                            value: OperationValue::Scalar(serde_json::json!("hello")),
                        },
                    }],
                },
            )
            .unwrap();

        assert!(receiver.decode_sync_frame(secure).is_err());
    }
}
