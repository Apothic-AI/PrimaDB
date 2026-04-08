use crate::clock::now_millis;
use crate::crypto::{EncryptedPayload, Identity, PublicIdentity, SecretBoxKey};
use crate::error::{PrimadbError, Result};
use crate::{DatabaseSnapshot, Operation, SignedPayload, SyncFrame};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;

const SEA_PREFIX: &str = "SEA";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedValueClaims {
    pub path: String,
    pub value: JsonValue,
    #[serde(default)]
    pub cert: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataCertificate {
    #[serde(rename = "c")]
    pub certificants: Vec<String>,
    #[serde(rename = "w")]
    pub write_policy: JsonValue,
    #[serde(rename = "e")]
    #[serde(default)]
    pub expires_at_millis: Option<u64>,
    #[serde(rename = "wb")]
    #[serde(default)]
    pub write_block: Option<JsonValue>,
    #[serde(default = "now_millis")]
    pub iat: u64,
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
        roots.into_iter().all(|root| {
            self.grants
                .iter()
                .any(|grant| grant.write && grant.matches_root(root))
        })
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
        self.trusted_users.insert(
            alias.clone(),
            UserRecord::new(alias, public_identity).with_grants(grants),
        );
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

    pub fn local_public_key(&self) -> Option<String> {
        self.local_user.as_ref().map(LocalUser::public_key)
    }

    pub fn certify_write(
        &self,
        certificants: Vec<String>,
        write_policy: JsonValue,
        expires_at_millis: Option<u64>,
        write_block: Option<JsonValue>,
    ) -> Result<String> {
        let local_user = self.local_user.as_ref().ok_or_else(|| {
            PrimadbError::Crypto(
                "cannot create a certificate without an authenticated user".to_owned(),
            )
        })?;
        let signed = local_user.identity.sign_payload(DataCertificate {
            certificants: normalize_certificants(certificants),
            write_policy,
            expires_at_millis,
            write_block,
            iat: now_millis(),
        })?;
        encode_sea_envelope(json!({
            "kind": "signed",
            "signed": signed,
        }))
    }

    pub fn sign_data_value(
        &self,
        path: &str,
        value: JsonValue,
        cert: Option<String>,
    ) -> Result<JsonValue> {
        let local_user = self.local_user.as_ref().ok_or_else(|| {
            PrimadbError::Crypto(
                "cannot sign a field value without an authenticated user".to_owned(),
            )
        })?;
        let signed = local_user.identity.sign_payload(SignedValueClaims {
            path: path.to_owned(),
            value,
            cert,
        })?;
        Ok(JsonValue::String(encode_sea_envelope(json!({
            "kind": "signed",
            "signed": signed,
        }))?))
    }

    pub fn verify_data_value(
        &self,
        expected_path: &str,
        value: &JsonValue,
    ) -> Result<Option<JsonValue>> {
        let Some(signed) = decode_signed_value(value)? else {
            return Ok(Some(value.clone()));
        };
        let claims = signed.verify()?;
        if claims.path != expected_path {
            return Ok(None);
        }

        if let Some(owner_pub) = owner_public_key_for_path(expected_path) {
            if signed.signer != owner_pub {
                let Some(certificate) = claims.cert.as_deref() else {
                    return Ok(None);
                };
                validate_certificate(certificate, &owner_pub, &signed.signer, expected_path)?;
            }
        }

        Ok(Some(claims.value))
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
                user.public_identity()?
                    .verify_payload(&crate::SignedPayload {
                        signer: frame.signer,
                        signature: frame.signature,
                        payload: (frame.claims.clone(), frame.frame.clone()),
                    })?;
                authorize_roots(user, &frame.claims, roots_for_frame(&frame.frame))?;
                Ok(frame.frame)
            }
            SecureSyncFrame::Encrypted(frame) => {
                let user = self.verify_claims(&frame.claims, &frame.signer)?;
                user.public_identity()?
                    .verify_payload(&crate::SignedPayload {
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
                        "received encrypted snapshot but no snapshot key is configured".to_owned(),
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

pub fn owner_public_key_for_path(path: &str) -> Option<String> {
    let root = path.split('/').next().unwrap_or(path);
    if !root.starts_with('~') || root.starts_with("~@") {
        return None;
    }
    Some(root.trim_start_matches('~').to_owned())
}

fn encode_sea_envelope(value: JsonValue) -> Result<String> {
    Ok(format!("{SEA_PREFIX}{}", serde_json::to_string(&value)?))
}

fn parse_sea_envelope(value: &str) -> Option<JsonValue> {
    if !value.starts_with(SEA_PREFIX) {
        return None;
    }
    serde_json::from_str(&value[SEA_PREFIX.len()..]).ok()
}

fn decode_signed_value(value: &JsonValue) -> Result<Option<SignedPayload<SignedValueClaims>>> {
    let JsonValue::String(value) = value else {
        return Ok(None);
    };
    let Some(envelope) = parse_sea_envelope(value) else {
        return Ok(None);
    };
    let signed = match envelope {
        JsonValue::Object(object) => object
            .get("signed")
            .cloned()
            .unwrap_or(JsonValue::Object(object)),
        other => other,
    };
    Ok(Some(serde_json::from_value(signed)?))
}

fn decode_certificate(value: &str) -> Result<SignedPayload<DataCertificate>> {
    let envelope = parse_sea_envelope(value).ok_or_else(|| {
        PrimadbError::Crypto("certificate is not a SEA-encoded signed payload".to_owned())
    })?;
    let signed = match envelope {
        JsonValue::Object(object) => object
            .get("signed")
            .cloned()
            .unwrap_or(JsonValue::Object(object)),
        other => other,
    };
    Ok(serde_json::from_value(signed)?)
}

fn normalize_certificants(certificants: Vec<String>) -> Vec<String> {
    if certificants.is_empty() {
        vec!["*".to_owned()]
    } else {
        certificants
    }
}

fn matches_lex(candidate: &str, pattern: &str) -> bool {
    if pattern.is_empty() || pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return candidate.starts_with(prefix);
    }
    candidate == pattern
}

fn matches_certificate_path(path: &str, policy: &JsonValue) -> bool {
    if policy.is_null() {
        return true;
    }

    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let key = segments.last().copied().unwrap_or_default();
    let soul_path = if segments.len() > 2 {
        segments[1..segments.len() - 1].join("/")
    } else {
        String::new()
    };

    let policies = match policy {
        JsonValue::Array(entries) => entries.clone(),
        other => vec![other.clone()],
    };

    policies.into_iter().any(|entry| match entry {
        JsonValue::String(pattern) => {
            matches_lex(path, &pattern)
                || matches_lex(&soul_path, &pattern)
                || matches_lex(key, &pattern)
        }
        JsonValue::Object(object) => {
            let path_rule = object.get("#").and_then(JsonValue::as_str);
            let field_rule = object.get(".").and_then(JsonValue::as_str);
            match (path_rule, field_rule) {
                (Some(path_rule), Some(field_rule)) => {
                    matches_lex(&soul_path, path_rule) && matches_lex(key, field_rule)
                }
                (Some(path_rule), None) => {
                    matches_lex(path, path_rule) || matches_lex(&soul_path, path_rule)
                }
                (None, Some(field_rule)) => matches_lex(key, field_rule),
                (None, None) => false,
            }
        }
        _ => false,
    })
}

fn validate_certificate(
    certificate: &str,
    owner_pub: &str,
    certificant: &str,
    expected_path: &str,
) -> Result<()> {
    let signed = decode_certificate(certificate)?;
    if signed.signer != owner_pub {
        return Err(PrimadbError::Crypto(
            "certificate signer does not match the path owner".to_owned(),
        ));
    }

    let claims = signed.verify()?;
    if claims
        .expires_at_millis
        .is_some_and(|expires_at| expires_at < now_millis())
    {
        return Err(PrimadbError::Crypto("certificate has expired".to_owned()));
    }
    if !claims
        .certificants
        .iter()
        .any(|candidate| candidate == "*" || candidate == certificant)
    {
        return Err(PrimadbError::Crypto(
            "certificate does not authorize this writer".to_owned(),
        ));
    }
    if !matches_certificate_path(expected_path, &claims.write_policy) {
        return Err(PrimadbError::Crypto(
            "certificate does not authorize this path".to_owned(),
        ));
    }
    if claims
        .write_block
        .as_ref()
        .is_some_and(|policy| matches_certificate_path(expected_path, policy))
    {
        return Err(PrimadbError::Crypto(
            "certificate blocks writes to this path".to_owned(),
        ));
    }
    Ok(())
}

fn authorize_roots(
    user: &UserRecord,
    claims: &AuthClaims,
    actual_roots: Vec<String>,
) -> Result<()> {
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
    use super::{SecureSyncFrame, SecurityState, UserGrant, owner_public_key_for_path};
    use crate::{Identity, Operation, OperationAction, OperationValue, Revision, SyncFrame};
    use serde_json::json;

    #[test]
    fn signed_frames_round_trip() {
        let identity = Identity::generate();
        let mut state = SecurityState::default();
        state
            .set_local_user(
                "alice",
                identity.clone(),
                vec![UserGrant::write_root("docs")],
            )
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
            .set_local_user(
                "alice",
                identity.clone(),
                vec![UserGrant::write_root("docs")],
            )
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

    #[test]
    fn owner_public_key_is_resolved_from_user_paths() {
        assert_eq!(
            owner_public_key_for_path("~alice-pub/profile/name"),
            Some("alice-pub".to_owned())
        );
        assert_eq!(owner_public_key_for_path("docs/post/title"), None);
        assert_eq!(owner_public_key_for_path("~@alice"), None);
    }

    #[test]
    fn signed_field_values_round_trip_through_core_helpers() {
        let identity = Identity::generate();
        let mut state = SecurityState::default();
        state
            .set_local_user("alice", identity.clone(), vec![UserGrant::write_root("*")])
            .unwrap();
        state.register_user(
            "alice",
            &identity.public_identity(),
            vec![UserGrant::write_root("*")],
        );

        let signed = state
            .sign_data_value(
                &format!("~{}/profile/display_name", identity.public_key_base64()),
                json!("Alice"),
                None,
            )
            .unwrap();

        let verified = state
            .verify_data_value(
                &format!("~{}/profile/display_name", identity.public_key_base64()),
                &signed,
            )
            .unwrap();
        assert_eq!(verified, Some(json!("Alice")));
    }

    #[test]
    fn delegated_certificates_authorize_signed_field_values() {
        let owner = Identity::generate();
        let delegate = Identity::generate();

        let mut owner_state = SecurityState::default();
        owner_state
            .set_local_user("owner", owner.clone(), vec![UserGrant::write_root("*")])
            .unwrap();
        owner_state.register_user(
            "owner",
            &owner.public_identity(),
            vec![UserGrant::write_root("*")],
        );
        let certificate = owner_state
            .certify_write(
                vec![delegate.public_key_base64()],
                json!({"#": "profile", ".": "tagline"}),
                None,
                None,
            )
            .unwrap();

        let mut delegate_state = SecurityState::default();
        delegate_state
            .set_local_user(
                "delegate",
                delegate.clone(),
                vec![UserGrant::write_root("*")],
            )
            .unwrap();
        let signed = delegate_state
            .sign_data_value(
                &format!("~{}/profile/tagline", owner.public_key_base64()),
                json!("delegated"),
                Some(certificate),
            )
            .unwrap();

        let mut verifier = SecurityState::default();
        verifier.register_user(
            "owner",
            &owner.public_identity(),
            vec![UserGrant::write_root("*")],
        );

        let verified = verifier
            .verify_data_value(
                &format!("~{}/profile/tagline", owner.public_key_base64()),
                &signed,
            )
            .unwrap();
        assert_eq!(verified, Some(json!("delegated")));
    }
}
