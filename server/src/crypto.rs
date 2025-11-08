use std::env;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use once_cell::sync::Lazy;
use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::elliptic_curve::rand_core::OsRng;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use tracing::{warn};

static GENERATED_KEY_NOTICE: Lazy<()> = Lazy::new(|| {
    warn!("No BARFFINE_CRYPTO_PRIVATE_KEY provided. Generated a transient key for doc RPC tokens.");
});

fn read_env_private_key() -> Option<String> {
    let vars = ["BARFFINE_CRYPTO_PRIVATE_KEY", "AFFINE_CRYPTO_PRIVATE_KEY"];

    for var in vars {
        if let Ok(value) = env::var(var) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    None
}

fn generate_private_key_pem() -> (SigningKey, String) {
    Lazy::force(&GENERATED_KEY_NOTICE);
    let signing_key = SigningKey::random(&mut OsRng);
    let pem = signing_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("failed to encode generated private key");
    (signing_key, pem.to_string())
}

fn load_private_key() -> (SigningKey, String) {
    if let Some(pem) = read_env_private_key() {
        match SigningKey::from_pkcs8_pem(&pem) {
            Ok(key) => (key, pem),
            Err(err) => {
                warn!(
                    ?err,
                    "failed to parse crypto private key, generating fallback keypair"
                );
                generate_private_key_pem()
            }
        }
    } else {
        generate_private_key_pem()
    }
}

/// Handles signing and verifying RPC access tokens that follow the AFFiNE format:
/// `<payload>,<base64-der-ecdsa-signature>`
pub(crate) struct DocTokenSigner {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    private_key_pem: String,
}

#[derive(Debug)]
pub(crate) enum DocTokenError {
    MalformedHeader,
    MissingDelimiter,
    InvalidSignature,
    InvalidEncoding,
}

impl DocTokenSigner {
    pub(crate) fn new() -> Self {
        let (signing_key, pem) = load_private_key();
        let verifying_key = VerifyingKey::from(&signing_key);

        Self {
            signing_key,
            verifying_key,
            private_key_pem: pem,
        }
    }

    pub(crate) fn sign(&self, payload: &str) -> String {
        let signature: Signature = self.signing_key.sign(payload.as_bytes());
        let encoded = BASE64.encode(signature.to_der());
        format!("{payload},{encoded}")
    }

    pub(crate) fn verify(&self, token: &str) -> Result<String, DocTokenError> {
        let (payload, signature) = token
            .split_once(',')
            .ok_or(DocTokenError::MissingDelimiter)?;
        let signature_bytes = BASE64
            .decode(signature.as_bytes())
            .map_err(|_| DocTokenError::InvalidEncoding)?;
        let signature =
            Signature::from_der(&signature_bytes).map_err(|_| DocTokenError::InvalidEncoding)?;
        self.verifying_key
            .verify(payload.as_bytes(), &signature)
            .map_err(|_| DocTokenError::InvalidSignature)?;
        Ok(payload.to_owned())
    }

    #[allow(dead_code)]
    pub(crate) fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }
}
