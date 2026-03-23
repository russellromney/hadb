//! Shared S3 error handling utilities.

use aws_smithy_types::error::metadata::ProvideErrorMetadata;

/// Check if an S3 error is a 412 PreconditionFailed (CAS conflict).
///
/// Uses proper SDK error metadata instead of fragile string matching.
/// Checks error code first (most reliable), then HTTP status code as fallback.
pub(crate) fn is_precondition_failed(
    err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
) -> bool {
    // Check error code from service error metadata.
    if let Some(service_err) = err.as_service_error() {
        if let Some(code) = service_err.code() {
            if code == "PreconditionFailed" || code == "ConditionalRequestConflict" {
                return true;
            }
        }
    }
    // Fallback: check HTTP status code.
    if let Some(raw) = err.raw_response() {
        if raw.status().as_u16() == 412 {
            return true;
        }
    }
    false
}

/// Check if an S3 error is a 404 NoSuchKey (key doesn't exist).
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
