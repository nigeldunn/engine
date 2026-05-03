//! Per-action `execute` and `probe` implementations. One module per kind so
//! each action's POST/GET flow stays self-contained.

pub mod close_pr;
pub mod commit_patch;
pub mod ensure_branch;
pub mod open_pr;
pub mod set_pr_status;
pub mod update_pr_metadata;
