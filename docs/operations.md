# Health, metrics, diagnostics and offline packaging

## Health and metrics

```sh
hashi run --config hashi.toml --listen 127.0.0.1:9464
```

`--listen` serves plain HTTP on the given address. Bind it to loopback or a
management interface; it has no authentication.

- `GET /healthz`: `200` once every instance has run and none failed its last
  pass, `503` otherwise (including before the first pass).
- `GET /metrics`: Prometheus text, one series per instance (`instance` label):

| Metric | Type | Meaning |
| --- | --- | --- |
| `hashi_records_delivered_total` | counter | Records acknowledged by the sink |
| `hashi_records_quarantined_total` | counter | Records moved to quarantine |
| `hashi_pass_errors_total` | counter | Passes that ended in an error |
| `hashi_records_per_second` | gauge | Records committed per second since the previous pass |
| `hashi_checkpoint_position` | gauge | Last committed position |
| `hashi_checkpoint_age_seconds` | gauge | Seconds since this process last advanced the checkpoint; absent until it does |
| `hashi_source_lag_seconds` | gauge | Age of the source timestamp of the newest committed record |
| `hashi_up` | gauge | `1` when the last pass succeeded |

Counters restart at zero with the process. `hashi run` sinks write straight to
their target, so there is no store-and-forward buffer to report yet; a buffered
sink adds its fill level when it becomes configurable.

## Diagnostics bundle

```sh
hashi diagnostics --config hashi.toml > hashi-diagnostics.json
```

One JSON document: the hashi and contract versions and, per instance, the
source and sink family, the committed position, whether a source cursor is
stored, quarantine counts by reason, and the schema revision and drift state of
each record kind. It reads state without locking or writing it, so it is safe
next to a running process.

It never reads credential files and leaves out credential paths and digests,
source cursors, record keys, payloads and local paths. Quarantine reasons
written by the runtime or schema drift are kept; a sink's own rejection text
may quote the record, so it is counted as `rejected by the sink`. The bundle
can be handed to support from an air-gapped site; an unreadable state file is
reported as such, without the parser's message.

## Offline packaging

`Dockerfile` builds one statically linked binary (musl) in a digest-pinned
build image and copies it onto an empty base, running as an unprivileged user.
The build needs crate downloads; the image needs nothing at run time:

```sh
docker build -t hashi .
docker run --network none -v "$PWD/site:/work" hashi run --config /work/hashi.toml
```

The binary opens no connections besides configured sources and sinks and the
optional `--listen` socket. `scripts/test-offline-image.sh` proves this by
running a fixture config in the image with `--network none`.
