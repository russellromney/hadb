//! Error classification helpers for S3 operations.
//!
//! S3-compatible services surface CAS conflicts and missing-key conditions
//! via HTTP status + error code. We check the SDK's structured error first
//! (most reliable) and fall back to HTTP status.

use aws_smithy_types::error::metadata::ProvideErrorMetadata;

/// `true` if the error is a 412 Precondition Failed (CAS conflict).
pub(crate) fn is_precondition_failed(
    err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
) -> bool {
    if let Some(service_err) = err.as_service_error() {
        if let Some(code) = service_err.code() {
            if code == "PreconditionFailed" || code == "ConditionalRequestConflict" {
                return true;
            }
        }
    }
    if let Some(raw) = err.raw_response() {
        if raw.status().as_u16() == 412 {
            return true;
        }
    }
    false
}

/// `true` if the error is a 404 / NoSuchKey / NotFound (key doesn't exist).
pub(crate) fn is_not_found(
    err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
) -> bool {
    if let Some(service_err) = err.as_service_error() {
        if let Some(code) = service_err.code() {
            if code == "NoSuchKey" || code == "NotFound" {
                return true;
            }
        }
    }
    if let Some(raw) = err.raw_response() {
        if raw.status().as_u16() == 404 {
            return true;
        }
    }
    false
}
