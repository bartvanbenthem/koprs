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


The strongest version of koprs would drop the 3-variant convenience wrappers for simple operations (apply, delete, get), keep the scope abstraction for the functions that actually benefit from it (GC, wait, ensure), and concentrate documentation energy on the genuinely hard parts. That would be a smaller, sharper library where every exported symbol clearly earns its place.