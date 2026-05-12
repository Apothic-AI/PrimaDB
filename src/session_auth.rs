use crate::clock::now_millis;
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
use crate::error::{PrimadbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn default_true() -> bool {
    true
}

fn default_challenge_timeout_ms() -> u64 {
    10_000
}

fn default_session_ttl_ms() -> u64 {
    5 * 60_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionAuthConfig {
    #[serde(default)]
    pub require_authenticated_peers: bool,
    #[serde(default)]
    pub trusted_public_keys: Vec<String>,
    #[serde(default)]
    pub trusted_aliases: Vec<String>,
    #[serde(default = "default_challenge_timeout_ms")]
    pub challenge_timeout_ms: u64,
    #[serde(default = "default_session_ttl_ms")]
    pub session_ttl_ms: u64,
    #[serde(default = "default_true")]
    pub allow_unauthenticated_presence: bool,
}

impl Default for SessionAuthConfig {
    fn default() -> Self {
        Self {
            require_authenticated_peers: false,
            trusted_public_keys: Vec::new(),
            trusted_aliases: Vec::new(),
            challenge_timeout_ms: default_challenge_timeout_ms(),
            session_ttl_ms: default_session_ttl_ms(),
            allow_unauthenticated_presence: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PresenceIdentity {
    pub public_key: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default = "default_key_scheme")]
    pub key_scheme: String,
    pub session_id: String,
    #[serde(default)]
    pub claims: BTreeMap<String, String>,
    #[serde(default = "now_millis")]
    pub issued_at_millis: u64,
    #[serde(default)]
    pub expires_at_millis: Option<u64>,
}

fn default_key_scheme() -> String {
    "ed25519".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdentityTrust {
    Verified,
    TrustedPublicKey,
    TrustedAlias,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedIdentity {
    pub public_key: String,
    #[serde(default)]
    pub alias: Option<String>,
    pub peer_id: String,
    pub replica_id: String,
    pub transport: String,
    pub session_id: String,
    #[serde(default)]
    pub claims: BTreeMap<String, String>,
    pub issued_at_millis: u64,
    #[serde(default)]
    pub expires_at_millis: Option<u64>,
    pub trust: IdentityTrust,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthChallenge {
    pub challenge_id: String,
    pub nonce: String,
    pub issuer_peer_id: String,
    pub issuer_replica_id: String,
    pub target_peer_id: String,
    pub target_replica_id: String,
    pub transport: String,
    pub issuer_session_id: String,
    pub target_session_id: String,
    pub issued_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthTranscript {
    pub protocol_version: u16,
    pub challenge_id: String,
    pub issuer_peer_id: String,
    pub issuer_replica_id: String,
    pub responder_peer_id: String,
    pub responder_replica_id: String,
    pub issuer_session_id: String,
    pub responder_session_id: String,
    pub issuer_nonce: String,
    pub responder_nonce: String,
    pub transport: String,
    pub responder_public_key: String,
    #[serde(default)]
    pub responder_alias: Option<String>,
    #[serde(default)]
    pub claims: BTreeMap<String, String>,
    pub issued_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthResponse {
    pub challenge_id: String,
    pub responder_peer_id: String,
    pub responder_replica_id: String,
    pub responder_identity: PresenceIdentity,
    pub responder_nonce: String,
    pub transcript: AuthTranscript,
    pub signer: String,
    pub signature: String,
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn random_session_id(prefix: &str) -> String {
    format!("{prefix}/session/{}", random_token())
}

#[cfg(all(
    not(feature = "crypto"),
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn random_session_id(prefix: &str) -> String {
    format!("{prefix}/session/{:x}", now_millis())
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn challenge_is_expired(challenge: &AuthChallenge) -> bool {
    now_millis() > challenge.expires_at_millis
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn create_auth_challenge(
    issuer_peer_id: &str,
    issuer_replica_id: &str,
    issuer_session_id: &str,
    target_peer_id: &str,
    target_replica_id: &str,
    target_identity: &PresenceIdentity,
    transport: &str,
    config: &SessionAuthConfig,
) -> AuthChallenge {
    let issued_at_millis = now_millis();
    AuthChallenge {
        challenge_id: format!("{issuer_peer_id}/challenge/{}", random_token()),
        nonce: random_token(),
        issuer_peer_id: issuer_peer_id.to_owned(),
        issuer_replica_id: issuer_replica_id.to_owned(),
        target_peer_id: target_peer_id.to_owned(),
        target_replica_id: target_replica_id.to_owned(),
        transport: transport.to_owned(),
        issuer_session_id: issuer_session_id.to_owned(),
        target_session_id: target_identity.session_id.clone(),
        issued_at_millis,
        expires_at_millis: issued_at_millis.saturating_add(config.challenge_timeout_ms.max(1)),
    }
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn sign_auth_response(
    identity: &crate::Identity,
    alias: Option<String>,
    claims: BTreeMap<String, String>,
    challenge: &AuthChallenge,
    responder_peer_id: &str,
    responder_replica_id: &str,
    responder_session_id: &str,
    config: &SessionAuthConfig,
) -> Result<AuthResponse> {
    if challenge_is_expired(challenge) {
        return Err(PrimadbError::Crypto(
            "session auth challenge has expired".to_owned(),
        ));
    }

    let issued_at_millis = now_millis();
    let responder_nonce = random_token();
    let public_key = identity.public_key_base64();
    let transcript = AuthTranscript {
        protocol_version: 1,
        challenge_id: challenge.challenge_id.clone(),
        issuer_peer_id: challenge.issuer_peer_id.clone(),
        issuer_replica_id: challenge.issuer_replica_id.clone(),
        responder_peer_id: responder_peer_id.to_owned(),
        responder_replica_id: responder_replica_id.to_owned(),
        issuer_session_id: challenge.issuer_session_id.clone(),
        responder_session_id: responder_session_id.to_owned(),
        issuer_nonce: challenge.nonce.clone(),
        responder_nonce: responder_nonce.clone(),
        transport: challenge.transport.clone(),
        responder_public_key: public_key.clone(),
        responder_alias: alias.clone(),
        claims: claims.clone(),
        issued_at_millis,
        expires_at_millis: issued_at_millis.saturating_add(config.session_ttl_ms.max(1)),
    };
    let signed = identity.sign_payload(transcript.clone())?;
    let responder_identity = PresenceIdentity {
        public_key,
        alias,
        key_scheme: default_key_scheme(),
        session_id: responder_session_id.to_owned(),
        claims,
        issued_at_millis,
        expires_at_millis: Some(transcript.expires_at_millis),
    };

    Ok(AuthResponse {
        challenge_id: challenge.challenge_id.clone(),
        responder_peer_id: responder_peer_id.to_owned(),
        responder_replica_id: responder_replica_id.to_owned(),
        responder_identity,
        responder_nonce,
        transcript,
        signer: signed.signer,
        signature: signed.signature,
    })
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn verify_auth_response(
    challenge: &AuthChallenge,
    response: &AuthResponse,
    config: &SessionAuthConfig,
) -> Result<VerifiedIdentity> {
    if challenge_is_expired(challenge) {
        return Err(PrimadbError::Crypto(
            "session auth challenge has expired".to_owned(),
        ));
    }
    if response.challenge_id != challenge.challenge_id {
        return Err(PrimadbError::Crypto(
            "session auth response challenge id mismatch".to_owned(),
        ));
    }
    if response.responder_peer_id != challenge.target_peer_id
        || response.responder_replica_id != challenge.target_replica_id
    {
        return Err(PrimadbError::Crypto(
            "session auth response peer identity mismatch".to_owned(),
        ));
    }
    if response.signer != response.responder_identity.public_key {
        return Err(PrimadbError::Crypto(
            "session auth response signer does not match advertised public key".to_owned(),
        ));
    }
    let expected_transcript = AuthTranscript {
        protocol_version: 1,
        challenge_id: challenge.challenge_id.clone(),
        issuer_peer_id: challenge.issuer_peer_id.clone(),
        issuer_replica_id: challenge.issuer_replica_id.clone(),
        responder_peer_id: response.responder_peer_id.clone(),
        responder_replica_id: response.responder_replica_id.clone(),
        issuer_session_id: challenge.issuer_session_id.clone(),
        responder_session_id: challenge.target_session_id.clone(),
        issuer_nonce: challenge.nonce.clone(),
        responder_nonce: response.responder_nonce.clone(),
        transport: challenge.transport.clone(),
        responder_public_key: response.responder_identity.public_key.clone(),
        responder_alias: response.responder_identity.alias.clone(),
        claims: response.responder_identity.claims.clone(),
        issued_at_millis: response.transcript.issued_at_millis,
        expires_at_millis: response.transcript.expires_at_millis,
    };
    if response.transcript != expected_transcript {
        return Err(PrimadbError::Crypto(
            "session auth response transcript mismatch".to_owned(),
        ));
    }
    if now_millis() > response.transcript.expires_at_millis {
        return Err(PrimadbError::Crypto(
            "session auth response has already expired".to_owned(),
        ));
    }

    let public_identity = crate::PublicIdentity::from_base64(&response.signer)?;
    public_identity.verify_payload(&crate::SignedPayload {
        signer: response.signer.clone(),
        signature: response.signature.clone(),
        payload: response.transcript.clone(),
    })?;

    let trust = resolve_trust(&response.responder_identity, config)?;
    Ok(VerifiedIdentity {
        public_key: response.responder_identity.public_key.clone(),
        alias: response.responder_identity.alias.clone(),
        peer_id: response.responder_peer_id.clone(),
        replica_id: response.responder_replica_id.clone(),
        transport: challenge.transport.clone(),
        session_id: response.responder_identity.session_id.clone(),
        claims: response.responder_identity.claims.clone(),
        issued_at_millis: response.transcript.issued_at_millis,
        expires_at_millis: Some(response.transcript.expires_at_millis),
        trust,
    })
}

#[cfg(all(
    not(feature = "crypto"),
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
pub fn verify_auth_response(
    _challenge: &AuthChallenge,
    _response: &AuthResponse,
    _config: &SessionAuthConfig,
) -> Result<VerifiedIdentity> {
    Err(PrimadbError::Message(
        "session authentication requires the crypto feature".to_owned(),
    ))
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
fn resolve_trust(identity: &PresenceIdentity, config: &SessionAuthConfig) -> Result<IdentityTrust> {
    let trust_lists_are_empty =
        config.trusted_public_keys.is_empty() && config.trusted_aliases.is_empty();
    if config
        .trusted_public_keys
        .iter()
        .any(|candidate| candidate == &identity.public_key)
    {
        return Ok(IdentityTrust::TrustedPublicKey);
    }
    if let Some(alias) = &identity.alias {
        if config
            .trusted_aliases
            .iter()
            .any(|candidate| candidate == alias)
        {
            return Ok(IdentityTrust::TrustedAlias);
        }
    }
    if trust_lists_are_empty {
        return Ok(IdentityTrust::Verified);
    }
    Err(PrimadbError::Crypto(
        "verified session identity is not trusted by this transport".to_owned(),
    ))
}

#[cfg(all(
    feature = "crypto",
    any(test, target_arch = "wasm32", feature = "native-websocket")
))]
fn random_token() -> String {
    use base64ct::{Base64UrlUnpadded, Encoding};
    use rand_core::{OsRng, RngCore};

    let mut bytes = [0_u8; 24];
    OsRng.fill_bytes(&mut bytes);
    Base64UrlUnpadded::encode_string(&bytes)
}

#[cfg(all(test, feature = "crypto"))]
mod tests {
    use super::*;
    use crate::Identity;

    fn presence_for(identity: &Identity, alias: &str, session_id: &str) -> PresenceIdentity {
        PresenceIdentity {
            public_key: identity.public_key_base64(),
            alias: Some(alias.to_owned()),
            key_scheme: "ed25519".to_owned(),
            session_id: session_id.to_owned(),
            claims: [("role".to_owned(), "test".to_owned())]
                .into_iter()
                .collect(),
            issued_at_millis: now_millis(),
            expires_at_millis: None,
        }
    }

    fn signed_response(
        config: &SessionAuthConfig,
    ) -> (AuthChallenge, AuthResponse, PresenceIdentity) {
        let responder = Identity::generate();
        let presence = presence_for(&responder, "alice", "responder-session");
        let challenge = create_auth_challenge(
            "issuer-peer",
            "issuer-replica",
            "issuer-session",
            "responder-peer",
            "responder-replica",
            &presence,
            "relay",
            config,
        );
        let response = sign_auth_response(
            &responder,
            presence.alias.clone(),
            presence.claims.clone(),
            &challenge,
            "responder-peer",
            "responder-replica",
            &presence.session_id,
            config,
        )
        .unwrap();
        (challenge, response, presence)
    }

    #[test]
    fn signed_challenge_response_verifies() {
        let config = SessionAuthConfig::default();
        let (challenge, response, presence) = signed_response(&config);

        let verified = verify_auth_response(&challenge, &response, &config).unwrap();

        assert_eq!(verified.public_key, presence.public_key);
        assert_eq!(verified.alias.as_deref(), Some("alice"));
        assert_eq!(verified.peer_id, "responder-peer");
        assert_eq!(verified.replica_id, "responder-replica");
        assert_eq!(verified.transport, "relay");
        assert_eq!(verified.trust, IdentityTrust::Verified);
    }

    #[test]
    fn transcript_tampering_is_rejected() {
        let config = SessionAuthConfig::default();
        let (challenge, mut response, _) = signed_response(&config);
        response.transcript.responder_peer_id = "attacker-peer".to_owned();

        let error = verify_auth_response(&challenge, &response, &config).unwrap_err();

        assert!(error.to_string().contains("transcript mismatch"));
    }

    #[test]
    fn expired_challenge_is_rejected() {
        let config = SessionAuthConfig::default();
        let (mut challenge, response, _) = signed_response(&config);
        challenge.expires_at_millis = now_millis().saturating_sub(1);

        let error = verify_auth_response(&challenge, &response, &config).unwrap_err();

        assert!(error.to_string().contains("expired"));
    }

    #[test]
    fn trust_lists_are_enforced_after_signature_verification() {
        let config = SessionAuthConfig {
            trusted_aliases: vec!["bob".to_owned()],
            ..SessionAuthConfig::default()
        };
        let (challenge, response, _) = signed_response(&config);

        let error = verify_auth_response(&challenge, &response, &config).unwrap_err();

        assert!(error.to_string().contains("not trusted"));
    }

    #[test]
    fn trusted_public_key_marks_identity_trusted() {
        let config = SessionAuthConfig::default();
        let (challenge, response, presence) = signed_response(&config);
        let trusted_config = SessionAuthConfig {
            trusted_public_keys: vec![presence.public_key.clone()],
            ..SessionAuthConfig::default()
        };

        let verified = verify_auth_response(&challenge, &response, &trusted_config).unwrap();

        assert_eq!(verified.trust, IdentityTrust::TrustedPublicKey);
    }
}
