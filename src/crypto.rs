use crate::SyncFrame;
use crate::error::{PrimadbError, Result};
use base64ct::{Base64UrlUnpadded, Encoding};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret as X25519StaticSecret};

const SECRET_BOX_HKDF_SALT_PREFIX: &[u8] = b"primadb:v1:sea:x25519:salt";
const SECRET_BOX_HKDF_INFO: &[u8] = b"primadb:v1:sea:xchacha20poly1305:json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedPayload<T> {
    pub signer: String,
    pub signature: String,
    pub payload: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EncryptedPayload {
    pub nonce: String,
    pub ciphertext: String,
}

#[derive(Debug, Clone)]
pub struct Identity {
    signing_key: SigningKey,
}

#[derive(Debug, Clone)]
pub struct PublicIdentity {
    verifying_key: VerifyingKey,
}

#[derive(Debug, Clone)]
pub struct SecretBoxKey {
    key_bytes: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeaPair {
    #[serde(rename = "pub")]
    pub public_key: String,
    pub epub: String,
    #[serde(rename = "priv")]
    pub secret_key: String,
    pub epriv: String,
}

impl Identity {
    pub fn generate() -> Self {
        Self {
            signing_key: SigningKey::generate(&mut OsRng),
        }
    }

    pub fn from_secret_key_bytes(secret_key: [u8; 32]) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(&secret_key),
        }
    }

    pub fn from_secret_key_base64(secret_key: &str) -> Result<Self> {
        let secret_key = decode_fixed::<32>(secret_key)?;
        Ok(Self::from_secret_key_bytes(secret_key))
    }

    pub fn public_identity(&self) -> PublicIdentity {
        PublicIdentity {
            verifying_key: self.signing_key.verifying_key(),
        }
    }

    pub fn public_key_base64(&self) -> String {
        Base64UrlUnpadded::encode_string(self.signing_key.verifying_key().as_bytes())
    }

    pub fn secret_key_base64(&self) -> String {
        Base64UrlUnpadded::encode_string(&self.signing_key.to_bytes())
    }

    pub fn sign_payload<T>(&self, payload: T) -> Result<SignedPayload<T>>
    where
        T: Serialize,
    {
        let payload_bytes = serde_json::to_vec(&payload)?;
        let signature = self.signing_key.sign(&payload_bytes);
        Ok(SignedPayload {
            signer: self.public_key_base64(),
            signature: Base64UrlUnpadded::encode_string(&signature.to_bytes()),
            payload,
        })
    }

    pub fn sign_sync_frame(&self, frame: SyncFrame) -> Result<SignedPayload<SyncFrame>> {
        self.sign_payload(frame)
    }
}

impl Default for Identity {
    fn default() -> Self {
        Self::generate()
    }
}

impl PublicIdentity {
    pub fn from_base64(public_key: &str) -> Result<Self> {
        let public_key = decode_fixed::<32>(public_key)?;
        let verifying_key = VerifyingKey::from_bytes(&public_key)
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
        Ok(Self { verifying_key })
    }

    pub fn to_base64(&self) -> String {
        Base64UrlUnpadded::encode_string(self.verifying_key.as_bytes())
    }

    pub fn verify_payload<T>(&self, signed: &SignedPayload<T>) -> Result<()>
    where
        T: Serialize,
    {
        let payload_bytes = serde_json::to_vec(&signed.payload)?;
        let signature_bytes = Base64UrlUnpadded::decode_vec(&signed.signature)
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
        let signature = Signature::from_slice(&signature_bytes)
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
        self.verifying_key
            .verify(&payload_bytes, &signature)
            .map_err(|error| PrimadbError::Crypto(error.to_string()))
    }
}

impl<T> SignedPayload<T>
where
    T: Serialize + Clone,
{
    pub fn verify(&self) -> Result<T> {
        let identity = PublicIdentity::from_base64(&self.signer)?;
        identity.verify_payload(self)?;
        Ok(self.payload.clone())
    }
}

impl SecretBoxKey {
    pub fn generate() -> Self {
        let mut key_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut key_bytes);
        Self { key_bytes }
    }

    pub fn from_bytes(key_bytes: [u8; 32]) -> Self {
        Self { key_bytes }
    }

    pub fn from_base64(encoded: &str) -> Result<Self> {
        let key_bytes = decode_fixed::<32>(encoded)?;
        Ok(Self { key_bytes })
    }

    pub fn to_base64(&self) -> String {
        Base64UrlUnpadded::encode_string(&self.key_bytes)
    }

    pub fn encrypt_json<T>(&self, payload: &T) -> Result<EncryptedPayload>
    where
        T: Serialize,
    {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key_bytes));
        let mut nonce_bytes = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let plaintext = serde_json::to_vec(payload)?;
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_ref())
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;

        Ok(EncryptedPayload {
            nonce: Base64UrlUnpadded::encode_string(&nonce_bytes),
            ciphertext: Base64UrlUnpadded::encode_string(&ciphertext),
        })
    }

    pub fn decrypt_json<T>(&self, payload: &EncryptedPayload) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key_bytes));
        let nonce_bytes = decode_fixed::<24>(&payload.nonce)?;
        let ciphertext = Base64UrlUnpadded::decode_vec(&payload.ciphertext)
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
        let plaintext = cipher
            .decrypt(XNonce::from_slice(&nonce_bytes), ciphertext.as_ref())
            .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
        serde_json::from_slice(&plaintext).map_err(Into::into)
    }
}

impl SeaPair {
    pub fn generate() -> Self {
        let identity = Identity::generate();
        let encryption_secret = X25519StaticSecret::random_from_rng(OsRng);
        let encryption_public = X25519PublicKey::from(&encryption_secret);
        Self {
            public_key: identity.public_key_base64(),
            epub: Base64UrlUnpadded::encode_string(encryption_public.as_bytes()),
            secret_key: identity.secret_key_base64(),
            epriv: Base64UrlUnpadded::encode_string(encryption_secret.as_bytes()),
        }
    }

    pub fn from_private_keys(secret_key: &str, encryption_secret_key: &str) -> Result<Self> {
        let identity = Identity::from_secret_key_base64(secret_key)?;
        let encryption_secret = x25519_secret_from_base64(encryption_secret_key)?;
        let encryption_public = X25519PublicKey::from(&encryption_secret);
        Ok(Self {
            public_key: identity.public_key_base64(),
            epub: Base64UrlUnpadded::encode_string(encryption_public.as_bytes()),
            secret_key: secret_key.to_owned(),
            epriv: encryption_secret_key.to_owned(),
        })
    }

    pub fn identity(&self) -> Result<Identity> {
        Identity::from_secret_key_base64(&self.secret_key)
    }

    pub fn public_identity(&self) -> Result<PublicIdentity> {
        PublicIdentity::from_base64(&self.public_key)
    }

    pub fn sign_payload<T>(&self, payload: T) -> Result<SignedPayload<T>>
    where
        T: Serialize,
    {
        self.identity()?.sign_payload(payload)
    }

    pub fn derive_secret_box(&self, other_epub: &str) -> Result<SecretBoxKey> {
        let encryption_secret = x25519_secret_from_base64(&self.epriv)?;
        let other_public = x25519_public_from_base64(other_epub)?;
        let shared = encryption_secret.diffie_hellman(&other_public);
        let local_public = X25519PublicKey::from(&encryption_secret);
        derive_secret_box_from_shared_secret(shared.as_bytes(), &local_public, &other_public)
    }
}

impl Default for SeaPair {
    fn default() -> Self {
        Self::generate()
    }
}

impl Default for SecretBoxKey {
    fn default() -> Self {
        Self::generate()
    }
}

fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N]> {
    let bytes = Base64UrlUnpadded::decode_vec(value)
        .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
    let actual = bytes.len();
    bytes.try_into().map_err(|_| {
        PrimadbError::Crypto(format!(
            "expected {} decoded bytes but received {}",
            N, actual
        ))
    })
}

fn x25519_secret_from_base64(value: &str) -> Result<X25519StaticSecret> {
    Ok(X25519StaticSecret::from(decode_fixed::<32>(value)?))
}

fn x25519_public_from_base64(value: &str) -> Result<X25519PublicKey> {
    Ok(X25519PublicKey::from(decode_fixed::<32>(value)?))
}

fn derive_secret_box_from_shared_secret(
    shared_secret: &[u8; 32],
    local_public: &X25519PublicKey,
    other_public: &X25519PublicKey,
) -> Result<SecretBoxKey> {
    let mut salt = Vec::with_capacity(SECRET_BOX_HKDF_SALT_PREFIX.len() + 64);
    salt.extend_from_slice(SECRET_BOX_HKDF_SALT_PREFIX);

    let local_bytes = local_public.as_bytes().as_slice();
    let other_bytes = other_public.as_bytes().as_slice();
    let (first, second) = if local_bytes <= other_bytes {
        (local_bytes, other_bytes)
    } else {
        (other_bytes, local_bytes)
    };
    salt.extend_from_slice(first);
    salt.extend_from_slice(second);

    let hkdf = Hkdf::<Sha256>::new(Some(&salt), shared_secret);
    let mut key_bytes = [0_u8; 32];
    hkdf.expand(SECRET_BOX_HKDF_INFO, &mut key_bytes)
        .map_err(|error| PrimadbError::Crypto(error.to_string()))?;
    Ok(SecretBoxKey::from_bytes(key_bytes))
}

#[cfg(test)]
mod tests {
    use super::{
        Identity, SeaPair, SecretBoxKey, x25519_public_from_base64, x25519_secret_from_base64,
    };
    use crate::SyncFrame;
    use serde_json::json;

    #[test]
    fn signed_payload_round_trips() {
        let identity = Identity::generate();
        let signed = identity
            .sign_sync_frame(SyncFrame::Ack {
                from: "peer-a".to_owned(),
                message_id: "m1".to_owned(),
                applied: 2,
            })
            .unwrap();

        let verified = signed.verify().unwrap();
        match verified {
            SyncFrame::Ack {
                from,
                message_id,
                applied,
            } => {
                assert_eq!(from, "peer-a");
                assert_eq!(message_id, "m1");
                assert_eq!(applied, 2);
            }
            _ => panic!("expected ack frame"),
        }
    }

    #[test]
    fn secret_box_encrypts_and_decrypts_json() {
        let key = SecretBoxKey::generate();
        let encrypted = key
            .encrypt_json(&json!({
                "title": "Encrypted",
                "done": false
            }))
            .unwrap();
        let decrypted: serde_json::Value = key.decrypt_json(&encrypted).unwrap();
        assert_eq!(decrypted["title"], "Encrypted");
        assert_eq!(decrypted["done"], false);
    }

    #[test]
    fn sea_pair_derives_shared_secret() {
        let alice = SeaPair::generate();
        let bob = SeaPair::generate();
        let alice_secret = alice.derive_secret_box(&bob.epub).unwrap();
        let bob_secret = bob.derive_secret_box(&alice.epub).unwrap();
        assert_eq!(alice_secret.to_base64(), bob_secret.to_base64());
    }

    #[test]
    fn sea_pair_secret_box_key_is_hkdf_output_not_raw_x25519_secret() {
        let alice = SeaPair::generate();
        let bob = SeaPair::generate();
        let derived = alice.derive_secret_box(&bob.epub).unwrap();

        let alice_secret = x25519_secret_from_base64(&alice.epriv).unwrap();
        let bob_public = x25519_public_from_base64(&bob.epub).unwrap();
        let raw_shared = alice_secret.diffie_hellman(&bob_public).to_bytes();
        let raw_key = SecretBoxKey::from_bytes(raw_shared);

        assert_ne!(derived.to_base64(), raw_key.to_base64());
    }
}
