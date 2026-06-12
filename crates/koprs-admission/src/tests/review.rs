// src/tests/review.rs
//
// Testing strategy
// ----------------
// All tests operate on raw serde_json::Value bodies so no HTTP server is
// required. parse_request, parse_uid, build_response, and build_deny_response
// are pure functions that are covered exhaustively here.

#[cfg(test)]
mod review_tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use crate::review::{
        Operation, ValidationResponse, build_deny_response, build_response, parse_request,
        parse_uid,
    };

    // -----------------------------------------------------------------------
    // Test resource
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Dummy {
        replicas: u32,
        name: String,
    }

    fn admission_review(operation: &str, object: Option<serde_json::Value>) -> serde_json::Value {
        let mut req = json!({
            "uid": "test-uid-1234",
            "name": "my-resource",
            "namespace": "my-namespace",
            "operation": operation,
            "dryRun": false,
        });
        if let Some(obj) = object {
            req["object"] = obj;
        }
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": req,
        })
    }

    // -----------------------------------------------------------------------
    // Operation
    // -----------------------------------------------------------------------

    #[test]
    fn operation_from_create_string() {
        assert_eq!(Operation::from_str("CREATE"), Operation::Create);
    }

    #[test]
    fn operation_from_update_string() {
        assert_eq!(Operation::from_str("UPDATE"), Operation::Update);
    }

    #[test]
    fn operation_from_delete_string() {
        assert_eq!(Operation::from_str("DELETE"), Operation::Delete);
    }

    #[test]
    fn operation_from_connect_string() {
        assert_eq!(Operation::from_str("CONNECT"), Operation::Connect);
    }

    #[test]
    fn operation_from_unknown_string_is_unknown_variant() {
        let op = Operation::from_str("PATCH");
        assert!(matches!(op, Operation::Unknown(ref s) if s == "PATCH"));
    }

    // -----------------------------------------------------------------------
    // parse_uid
    // -----------------------------------------------------------------------

    #[test]
    fn parse_uid_returns_uid_from_request() {
        let body = admission_review("CREATE", None);
        assert_eq!(parse_uid(&body), "test-uid-1234");
    }

    #[test]
    fn parse_uid_returns_empty_string_when_missing() {
        let body = json!({ "kind": "AdmissionReview" });
        assert_eq!(parse_uid(&body), "");
    }

    // -----------------------------------------------------------------------
    // parse_request
    // -----------------------------------------------------------------------

    #[test]
    fn parse_request_extracts_metadata() {
        let body = admission_review("CREATE", None);
        let req = parse_request::<Dummy>(&body).unwrap();
        assert_eq!(req.uid, "test-uid-1234");
        assert_eq!(req.name, "my-resource");
        assert_eq!(req.namespace.as_deref(), Some("my-namespace"));
        assert_eq!(req.operation, Operation::Create);
        assert!(!req.dry_run);
    }

    #[test]
    fn parse_request_deserializes_object_field() {
        let obj = json!({ "replicas": 3, "name": "my-app" });
        let body = admission_review("CREATE", Some(obj));
        let req = parse_request::<Dummy>(&body).unwrap();
        let object = req.object.expect("object should be present");
        assert_eq!(object.replicas, 3);
        assert_eq!(object.name, "my-app");
    }

    #[test]
    fn parse_request_object_is_none_when_absent() {
        let body = admission_review("DELETE", None);
        let req = parse_request::<Dummy>(&body).unwrap();
        assert!(req.object.is_none());
    }

    #[test]
    fn parse_request_old_object_is_none_when_absent() {
        let body = admission_review("CREATE", None);
        let req = parse_request::<Dummy>(&body).unwrap();
        assert!(req.old_object.is_none());
    }

    #[test]
    fn parse_request_old_object_is_parsed() {
        let old = json!({ "replicas": 1, "name": "old" });
        let mut body = admission_review("UPDATE", Some(json!({ "replicas": 2, "name": "new" })));
        body["request"]["oldObject"] = old;
        let req = parse_request::<Dummy>(&body).unwrap();
        assert_eq!(req.old_object.as_ref().map(|o| o.replicas), Some(1));
    }

    #[test]
    fn parse_request_dry_run_true_is_parsed() {
        let mut body = admission_review("CREATE", None);
        body["request"]["dryRun"] = json!(true);
        let req = parse_request::<Dummy>(&body).unwrap();
        assert!(req.dry_run);
    }

    #[test]
    fn parse_request_null_object_is_treated_as_none() {
        let mut body = admission_review("DELETE", None);
        body["request"]["object"] = json!(null);
        let req = parse_request::<Dummy>(&body).unwrap();
        assert!(req.object.is_none());
    }

    #[test]
    fn parse_request_returns_error_when_request_field_missing() {
        let body = json!({ "kind": "AdmissionReview" });
        let result = parse_request::<Dummy>(&body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_request_returns_error_on_malformed_object() {
        let bad_obj = json!({ "unexpected_field_only": true });
        // Dummy requires "replicas" and "name"; missing replicas fails
        let mut body = admission_review("CREATE", None);
        body["request"]["object"] = bad_obj;
        let result = parse_request::<Dummy>(&body);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // ValidationResponse
    // -----------------------------------------------------------------------

    #[test]
    fn validation_response_allow_is_allowed() {
        let r = ValidationResponse::allow();
        assert!(r.allowed);
        assert!(r.message.is_none());
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn validation_response_deny_is_not_allowed() {
        let r = ValidationResponse::deny("too many replicas");
        assert!(!r.allowed);
        assert_eq!(r.message.as_deref(), Some("too many replicas"));
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn validation_response_allow_with_warnings_is_allowed() {
        let r =
            ValidationResponse::allow_with_warnings(vec!["consider using fewer replicas".into()]);
        assert!(r.allowed);
        assert_eq!(r.warnings.len(), 1);
        assert!(r.warnings[0].contains("replicas"));
    }

    // -----------------------------------------------------------------------
    // build_response
    // -----------------------------------------------------------------------

    #[test]
    fn build_response_allow_sets_uid_and_allowed() {
        let resp = ValidationResponse::allow();
        let v = build_response("uid-abc", &resp);
        assert_eq!(v["response"]["uid"], "uid-abc");
        assert_eq!(v["response"]["allowed"], true);
        assert_eq!(v["apiVersion"], "admission.k8s.io/v1");
        assert_eq!(v["kind"], "AdmissionReview");
    }

    #[test]
    fn build_response_deny_includes_status_message() {
        let resp = ValidationResponse::deny("image tag latest not allowed");
        let v = build_response("uid-xyz", &resp);
        assert_eq!(v["response"]["allowed"], false);
        assert_eq!(
            v["response"]["status"]["message"],
            "image tag latest not allowed"
        );
        assert_eq!(v["response"]["status"]["code"], 403);
    }

    #[test]
    fn build_response_allow_omits_status_when_no_message() {
        let resp = ValidationResponse::allow();
        let v = build_response("uid-1", &resp);
        assert!(v["response"]["status"].is_null());
    }

    #[test]
    fn build_response_includes_warnings_when_present() {
        let resp = ValidationResponse::allow_with_warnings(vec!["w1".into(), "w2".into()]);
        let v = build_response("uid-2", &resp);
        assert!(v["response"]["warnings"].is_array());
        assert_eq!(v["response"]["warnings"][0], "w1");
        assert_eq!(v["response"]["warnings"][1], "w2");
    }

    #[test]
    fn build_response_omits_warnings_when_empty() {
        let resp = ValidationResponse::allow();
        let v = build_response("uid-3", &resp);
        assert!(v["response"]["warnings"].is_null());
    }

    // -----------------------------------------------------------------------
    // build_deny_response
    // -----------------------------------------------------------------------

    #[test]
    fn build_deny_response_sets_allowed_false() {
        let v = build_deny_response("uid-err", "parse error");
        assert_eq!(v["response"]["allowed"], false);
        assert_eq!(v["response"]["uid"], "uid-err");
        assert_eq!(v["response"]["status"]["code"], 400);
        assert_eq!(v["response"]["status"]["message"], "parse error");
    }

    #[test]
    fn build_deny_response_sets_correct_api_version() {
        let v = build_deny_response("x", "msg");
        assert_eq!(v["apiVersion"], "admission.k8s.io/v1");
        assert_eq!(v["kind"], "AdmissionReview");
    }
}
