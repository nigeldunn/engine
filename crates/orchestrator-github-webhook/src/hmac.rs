//! HMAC-SHA256 signature validation for GitHub webhook deliveries.
//!
//! GitHub signs every webhook with the App's webhook secret; the signature
//! comes in as `X-Hub-Signature-256: sha256=<hex>`. Validation is computed
//! against the **raw request body bytes** — not over a re-serialized JSON
//! value — so the router uses axum's `Bytes` extractor (NOT `Json`) and
//! preserves those bytes for both HMAC and JSON parsing.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::WebhookError;

const SIGNATURE_PREFIX: &str = "sha256=";

type HmacSha256 = Hmac<Sha256>;

/// Validate a GitHub webhook signature.
///
/// Returns `Ok(())` only when the HMAC computed from `body` and `secret`
/// matches the hex bytes in `signature_header` (after the `sha256=` prefix).
/// Comparison is constant-time via `subtle::ConstantTimeEq`.
pub fn validate_hmac_sha256(
    secret: &str,
    body: &[u8],
    signature_header: &str,
) -> Result<(), WebhookError> {
    let hex_part = signature_header
        .strip_prefix(SIGNATURE_PREFIX)
        .ok_or_else(|| {
            WebhookError::MalformedSignature(format!(
                "expected '{}' prefix",
                SIGNATURE_PREFIX
            ))
        })?;

    let provided = hex::decode(hex_part)
        .map_err(|e| WebhookError::MalformedSignature(format!("hex decode: {}", e)))?;

    // HMAC-SHA256 accepts any key length (RFC 2104).
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(body);
    let computed = mac.finalize().into_bytes();

    // ConstantTimeEq for slices handles unequal lengths in constant time
    // and returns false; signatures of the wrong length therefore fail
    // validation here, not via a separate length check that would leak
    // timing.
    if computed.as_slice().ct_eq(provided.as_slice()).into() {
        Ok(())
    } else {
        Err(WebhookError::SignatureMismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_signature(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn validates_correct_signature() {
        let secret = "shh";
        let body = b"payload bytes";
        let sig = known_signature(secret, body);
        validate_hmac_sha256(secret, body, &sig).unwrap();
    }

    #[test]
    fn validates_with_empty_body() {
        let secret = "shh";
        let body = b"";
        let sig = known_signature(secret, body);
        validate_hmac_sha256(secret, body, &sig).unwrap();
    }

    #[test]
    fn rejects_signature_with_one_flipped_hex_char() {
        let secret = "shh";
        let body = b"payload bytes";
        let mut sig = known_signature(secret, body);
        // Flip the last hex char.
        let last = sig.pop().unwrap();
        let flipped = if last == '0' { '1' } else { '0' };
        sig.push(flipped);
        let err = validate_hmac_sha256(secret, body, &sig).unwrap_err();
        assert!(matches!(err, WebhookError::SignatureMismatch));
    }

    #[test]
    fn rejects_signature_for_different_body() {
        let secret = "shh";
        let sig = known_signature(secret, b"original");
        let err = validate_hmac_sha256(secret, b"tampered", &sig).unwrap_err();
        assert!(matches!(err, WebhookError::SignatureMismatch));
    }

    #[test]
    fn rejects_signature_with_wrong_secret() {
        let body = b"payload";
        let sig = known_signature("right", body);
        let err = validate_hmac_sha256("wrong", body, &sig).unwrap_err();
        assert!(matches!(err, WebhookError::SignatureMismatch));
    }

    #[test]
    fn rejects_signature_missing_prefix() {
        let body = b"payload";
        let bad = "deadbeef".to_string();
        let err = validate_hmac_sha256("shh", body, &bad).unwrap_err();
        assert!(matches!(err, WebhookError::MalformedSignature(_)));
    }

    #[test]
    fn rejects_signature_with_invalid_hex() {
        let bad = "sha256=zzznotvalidhex".to_string();
        let err = validate_hmac_sha256("shh", b"x", &bad).unwrap_err();
        assert!(matches!(err, WebhookError::MalformedSignature(_)));
    }

    #[test]
    fn rejects_empty_signature_header() {
        let err = validate_hmac_sha256("shh", b"x", "").unwrap_err();
        assert!(matches!(err, WebhookError::MalformedSignature(_)));
    }

    #[test]
    fn rejects_signature_with_wrong_length_hex() {
        // Decodes to 4 bytes — not 32. ConstantTimeEq on unequal lengths
        // returns false, so this surfaces as SignatureMismatch.
        let bad = "sha256=deadbeef".to_string();
        let err = validate_hmac_sha256("shh", b"x", &bad).unwrap_err();
        assert!(matches!(err, WebhookError::SignatureMismatch));
    }

    #[test]
    fn validation_is_case_sensitive_on_hex() {
        // GitHub sends lowercase hex; uppercase decodes to the same bytes
        // and validates fine. Document that here.
        let secret = "shh";
        let body = b"x";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let bytes = mac.finalize().into_bytes();
        let upper = format!("sha256={}", hex::encode_upper(bytes));
        validate_hmac_sha256(secret, body, &upper).unwrap();
    }
}
