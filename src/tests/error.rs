// =========================================================================
// Error variants
// =========================================================================
#[cfg(test)]
mod tests {

    use crate::error::KubeGenericError;

    #[test]
    fn missing_metadata_error_displays_field_name() {
        let err = KubeGenericError::MissingMetadata("name".to_string());
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn internal_error_displays_message() {
        let err = KubeGenericError::Internal("something failed".to_string());
        assert!(err.to_string().contains("something failed"));
    }
}
