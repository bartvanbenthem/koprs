Your operator binary
│
├── uses  koprs::controller::ControllerBuilder   ← ties the loop together
├── uses  koprs::crd::apply_crd                  ← startup bootstrap
├── uses  koprs::health                          ← pod probes
├── uses  koprs::leader                          ← HA
├── uses  koprs::shutdown                        ← SIGTERM drain
│
├── annotates CRDs with  #[derive(KoResource)]  from koprs-derive
│     └── emits inventory registrations for koprs-gen
│
└── runs  cargo run --bin generate               ← koprs-gen writes manifests/

