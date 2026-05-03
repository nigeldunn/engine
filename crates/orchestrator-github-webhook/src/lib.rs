//! HMAC-validated GitHub webhook ingestion.
//!
//! Pipeline (per PLAN.md M10): inbound HTTP POST → required-header check
//! → HMAC-SHA256 validation against the raw body bytes → JSON parse →
//! consumer's handler closure (which translates the validated delivery
//! into an `EventCommand` and calls `executor.advance(...)`).
//!
//! The webhook crate is transport-only. Workflow routing — i.e.
//! mapping a delivery to a `workflow_id` — happens in the consumer's
//! handler, not here. Use `delivery.delivery_id` as
//! `EventCommand::ingress_dedup_key` so GitHub's at-least-once delivery
//! semantics are absorbed at `Storage::advance`.

pub mod delivery;
pub mod error;
pub mod hmac;
pub mod router;

pub use delivery::{parse_delivery, GithubWebhookDelivery};
pub use error::WebhookError;
pub use hmac::validate_hmac_sha256;
pub use router::{router, GithubWebhookConfig};
