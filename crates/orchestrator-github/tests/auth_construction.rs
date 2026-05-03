//! Unit tests for GithubAuth construction. PEM validation only — token
//! fetching requires real GitHub and lives in `integration_check_health.rs`.

use orchestrator_github::{GithubAuth, GithubAuthError};

#[test]
fn rejects_empty_pem() {
    let err = GithubAuth::new(123, "", 456).expect_err("empty PEM should fail");
    assert!(matches!(err, GithubAuthError::InvalidPem(_)));
}

#[test]
fn rejects_whitespace_only_pem() {
    let err = GithubAuth::new(123, "   \n\t  ", 456).expect_err("whitespace PEM should fail");
    assert!(matches!(err, GithubAuthError::InvalidPem(_)));
}

#[test]
fn rejects_malformed_pem() {
    let err =
        GithubAuth::new(123, "not a valid pem block", 456).expect_err("garbage PEM should fail");
    assert!(matches!(err, GithubAuthError::InvalidPem(_)));
}

#[test]
fn accepts_valid_pem_fixture() {
    let pem = include_str!("fixtures/test_app_key.pem");
    let auth = GithubAuth::new(12345, pem, 67890).expect("test fixture must parse");
    assert_eq!(auth.app_id(), 12345);
    assert_eq!(auth.installation_id(), 67890);
}

#[test]
fn app_jwt_is_signable_with_valid_key() {
    let pem = include_str!("fixtures/test_app_key.pem");
    let auth = GithubAuth::new(12345, pem, 67890).unwrap();
    let jwt = auth.app_jwt().expect("jwt signing must succeed with valid key");
    // JWT format: three base64url segments separated by '.'
    let segments: Vec<&str> = jwt.split('.').collect();
    assert_eq!(segments.len(), 3, "JWT must have 3 segments");
    for seg in &segments {
        assert!(!seg.is_empty(), "JWT segment must not be empty");
    }
}
