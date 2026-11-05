# Build from the workspace root:
#   docker build -f examples/apiconfigsync/Dockerfile -t apiconfigsync-operator:dev .
FROM rust:1.93-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p apiconfigsync-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/apiconfigsync-operator /apiconfigsync-operator
# Numeric UID (distroless "nonroot") so `runAsNonRoot: true` can be verified.
USER 65532:65532
ENTRYPOINT ["/apiconfigsync-operator"]
