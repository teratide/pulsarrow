# Pulsarrow

A tiny Pulsar + Arrow experiment.

## Quick start

### Requirements

- Rust
- Docker

### Start standalone Pulsar

```bash
docker run -it --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar bin/pulsar standalone
```

This starts a standalone Pulsar service available at `pulsar://localhost:6650`.

### Run application

```bash
cargo run pulsar://localhost:6650
```

Starts a producer and consumer with Arrow IPC messages.

:dolphin:

## License

Licensed under the Apache-2.0 license. See LICENSE.
