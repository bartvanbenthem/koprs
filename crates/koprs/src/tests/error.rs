// src/tests/error.rs

#[cfg(test)]
mod error_tests {
    use std::io;

    use crate::error::{KubeGenericError, Result};

    // -----------------------------------------------------------------------
    // Display — #[error("...")] format strings
    // -----------------------------------------------------------------------

    #[test]
    fn missing_metadata_display_contains_field_name() {
        let err = KubeGenericError::MissingMetadata("name".to_string());
        assert_eq!(err.to_string(), "Missing metadata field: name");
    }

    #[test]
    fn io_display_contains_underlying_message() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = KubeGenericError::Io(io_err);
        assert!(
            err.to_string().contains("file not found"),
            "display was: {err}"
        );
    }

    #[test]
    fn serialization_display_contains_underlying_message() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = KubeGenericError::Serialization(json_err);
        assert!(
            err.to_string().starts_with("Serialization error:"),
            "display was: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // From conversions — #[from] derives
    // -----------------------------------------------------------------------

    #[test]
    fn from_io_error_produces_io_variant() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let err = KubeGenericError::from(io_err);
        assert!(
            matches!(err, KubeGenericError::Io(_)),
            "expected Io variant, got: {err:?}"
        );
    }

    #[test]
    fn from_serde_json_error_produces_serialization_variant() {
        let json_err = serde_json::from_str::<serde_json::Value>("{bad}").unwrap_err();
        let err = KubeGenericError::from(json_err);
        assert!(
            matches!(err, KubeGenericError::Serialization(_)),
            "expected Serialization variant, got: {err:?}"
        );
    }

    #[test]
    fn question_mark_converts_io_error_automatically() {
        fn fallible() -> Result<()> {
            let _ = std::fs::read("/this/path/does/not/exist/ever")?;
            Ok(())
        }
        let err = fallible().unwrap_err();
        assert!(
            matches!(err, KubeGenericError::Io(_)),
            "expected Io variant from ?, got: {err:?}"
        );
    }

    #[test]
    fn question_mark_converts_serde_json_error_automatically() {
        fn fallible() -> Result<serde_json::Value> {
            Ok(serde_json::from_str("not valid json")?)
        }
        let err = fallible().unwrap_err();
        assert!(
            matches!(err, KubeGenericError::Serialization(_)),
            "expected Serialization variant from ?, got: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Manually constructed variants
    // -----------------------------------------------------------------------

    #[test]
    fn missing_metadata_stores_field_name() {
        let err = KubeGenericError::MissingMetadata("namespace".to_string());
        if let KubeGenericError::MissingMetadata(field) = err {
            assert_eq!(field, "namespace");
        } else {
            panic!("expected MissingMetadata variant");
        }
    }

    // -----------------------------------------------------------------------
    // Result type alias
    // -----------------------------------------------------------------------

    #[test]
    fn result_ok_variant_works() {
        let r: Result<i32> = Ok(42);
        assert!(matches!(r, Ok(42)));
    }

    #[test]
    fn result_err_variant_carries_error() {
        let r: Result<()> = Err(KubeGenericError::MissingMetadata("oops".to_string()));
        assert!(r.is_err());
    }

    // -----------------------------------------------------------------------
    // Debug — all variants are reachable and format without panicking
    // -----------------------------------------------------------------------

    #[test]
    fn all_variants_implement_debug_without_panicking() {
        let variants: &[KubeGenericError] = &[
            KubeGenericError::MissingMetadata("x".to_string()),
            KubeGenericError::Io(io::Error::other("x")),
            KubeGenericError::Serialization(
                serde_json::from_str::<serde_json::Value>("!").unwrap_err(),
            ),
            KubeGenericError::Config("x".to_string()),
            KubeGenericError::LeaderElectionLost("x".to_string()),
        ];
        for v in variants {
            let _ = format!("{v:?}");
        }
    }

    // -----------------------------------------------------------------------
    // metric_label — bounded, data-free labels
    // -----------------------------------------------------------------------

    fn api_error(code: u16) -> KubeGenericError {
        KubeGenericError::Kube(kube::Error::Api(
            kube::core::Status::failure(&format!("object my-cr-{code} failed"), "Reason")
                .with_code(code)
                .boxed(),
        ))
    }

    #[test]
    fn metric_label_classifies_api_errors_by_status_code() {
        assert_eq!(api_error(404).metric_label(), "kube_not_found");
        assert_eq!(api_error(409).metric_label(), "kube_conflict");
        assert_eq!(api_error(503).metric_label(), "kube_server_error");
        assert_eq!(api_error(418).metric_label(), "kube_api_error");
    }

    #[test]
    fn metric_label_never_contains_error_message_data() {
        let err = KubeGenericError::MissingMetadata("very-specific-field".to_string());
        assert_eq!(err.metric_label(), "missing_metadata");
        assert!(!api_error(409).metric_label().contains("my-cr"));
    }
}
