//! Per-action `execute` and `probe` implementations. One module per kind so
//! each action's POST/GET flow stays self-contained.

pub mod commit_patch;
pub mod ensure_branch;
