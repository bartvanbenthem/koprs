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
        assert_eq!(r.unwrap(), 42);
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
            KubeGenericError::Io(io::Error::new(io::ErrorKind::Other, "x")),
            KubeGenericError::Serialization(
                serde_json::from_str::<serde_json::Value>("!").unwrap_err(),
            ),
        ];
        for v in variants {
            let _ = format!("{v:?}");
        }
    }
}
