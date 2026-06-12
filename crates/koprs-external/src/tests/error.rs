// src/tests/error.rs

#[cfg(test)]
mod error_tests {
    use crate::error::ExternalError;

    #[test]
    fn from_io_error_produces_io_variant() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = ExternalError::from(io_err);
        assert!(matches!(err, ExternalError::Io(_)));
        assert!(err.to_string().contains("I/O error"));
    }

    #[test]
    fn internal_error_display_includes_message() {
        let err = ExternalError::Internal("something broke".to_string());
        assert_eq!(err.to_string(), "Internal error: something broke");
    }

    #[test]
    fn error_is_debug_formattable() {
        let err = ExternalError::Internal("debug test".to_string());
        let s = format!("{err:?}");
        assert!(s.contains("Internal"));
    }
}
