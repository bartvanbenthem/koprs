// src/tests/error.rs

#[cfg(test)]
mod error_tests {
    use crate::error::AdmissionError;

    #[test]
    fn tls_error_display_includes_message() {
        let err = AdmissionError::Tls("certificate expired".to_string());
        assert_eq!(err.to_string(), "TLS error: certificate expired");
    }

    #[test]
    fn io_error_display_includes_message() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = AdmissionError::from(io_err);
        assert!(matches!(err, AdmissionError::Io(_)));
        assert!(err.to_string().contains("I/O error"));
    }

    #[test]
    fn serialization_error_display_includes_message() {
        let json_err = serde_json::from_str::<serde_json::Value>("{bad json").unwrap_err();
        let err = AdmissionError::from(json_err);
        assert!(matches!(err, AdmissionError::Serialization(_)));
        assert!(err.to_string().contains("Serialization error"));
    }

    #[test]
    fn internal_error_display_includes_message() {
        let err = AdmissionError::Internal("unexpected state".to_string());
        assert_eq!(err.to_string(), "Internal error: unexpected state");
    }

    #[test]
    fn error_is_debug_formattable() {
        let err = AdmissionError::Internal("debug test".to_string());
        let s = format!("{err:?}");
        assert!(s.contains("Internal"));
    }

    #[test]
    fn from_io_produces_io_variant() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: AdmissionError = io_err.into();
        assert!(matches!(err, AdmissionError::Io(_)));
    }
}
