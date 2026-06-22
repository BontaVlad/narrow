# Arrow Flight

Recipes for serving and consuming data over Apache Arrow Flight.

> **TODO:** narrow does not yet wrap Arrow Flight GLib (`arrow-flight-glib`).
> All recipes in this section are placeholders describing the planned API
> surface — none of the code below runs today. Track progress in the issue
> tracker.

.. contents::
----

## Simple Parquet storage service with Arrow Flight

> **TODO:** A minimal Flight server that reads Parquet files from disk and
> serves them as Flight endpoints. The planned API:
>
> ```nim
> let server = newFlightServer("grpc://0.0.0.0:0")
> server.registerHandler(ParquetStorageHandler(root = "/data"))
> server.serve()
> ```

## Streaming Parquet Storage Service

> **TODO:** A Flight service that streams large Parquet datasets in batches
> via `doGet`, so clients can consume row groups incrementally without
> loading the whole file into memory.

## Authentication with user/password

> **TODO:** Token-based authentication middleware for Flight servers —
> clients send credentials in the initial handshake and receive a bearer
> token for subsequent calls.

## Securing connections with TLS

> **TODO:** Configuring gRPC TLS for Flight servers and clients, including
> mutual-TLS setups. The planned API will accept a certificate / key pair
> on server construction.

## Propagating OpenTelemetry Traces

> **TODO:** Wiring OpenTelemetry trace context through Flight headers so
> that distributed spans follow requests across the Flight boundary. This
> depends on both Flight support and an OpenTelemetry integration in
> narrow.
