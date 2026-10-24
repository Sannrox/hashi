# Soak harness

`hashi::soak::soak` runs a family's conformance driver ([conformance.md](conformance.md))
for a configured duration. Each cycle produces one change and drains until it
arrives, one change at a time so a sampling source never overwrites one before
it is read. Faults are injected on a schedule through the family's
`soak::Faults` hooks, and the run ends with a final drain.

Every change produced must be delivered. One that never is, is a gap: the
report names it and the run fails. A connector that cannot be rebuilt after a
fault also fails the run.

## Faults

| Fault | What the fixture does | What the harness does |
| --- | --- | --- |
| `network_drop` | Stops answering (a paused container), then resumes | Keeps the connector reading; it must carry on once the fixture answers |
| `source_restart` | The source restarts and its sessions end | Rebuilds the connector from the durable checkpoint once the source is back |
| `credential_rotation` | The source credential changes | Rebuilds the connector with the new credential |

Before each fault the harness produces one change, so a change is in flight
when the fault hits, and it must still arrive. The fault is injected under the
live connector, which keeps reading through the fault window. Failed reads, and
reconnects that fail while the source is away, are expected there: they are
counted, never fatal. Outside a fault, a failed read rebuilds the connector from
the durable checkpoint, and repeated failed rebuilds end the run.

| Family | Fixture | Faults |
| --- | --- | --- |
| Modbus | In-process register simulator | Network drop and source restart (the device stops answering). No credential. |
| OPC UA | In-process simulator | Source restart (stopped and started on the same port, serving the current value). No pause; its user is fixed. |
| MQTT | Disposable broker with persistence | Network drop (`docker pause`) and source restart (`docker restart`, port re-read; persistence keeps acknowledged messages and sessions). The anonymous fixture has no credential. |
| MySQL | Disposable database | Network drop (`docker pause`), source restart (every session of the connector's user is killed; the data lives on tmpfs, so the container is not restarted), and credential rotation (`ALTER USER` on the connector's own user). |

The MySQL rotation runs with `sql_log_bin = 0`: the connector stops on account
statements in the binlog (#49), and this fault is about the new credential.

## Report

Each family writes one JSON report (`hashi.soak-report/v1`):

| Field | Meaning |
| --- | --- |
| `cycles`, `produced`, `delivered` | Changes produced and data records delivered, counting redeliveries |
| `throughput_per_sec` | Delivered records per second of the whole run |
| `lag_ms` | Produce-to-first-delivery lag: `p50`, `p95`, `p99`, `max` |
| `faults` | How often each fault was injected |
| `reconnects`, `read_errors`, `last_read_error` | Connector rebuilds and the reads that caused them |
| `gaps` | Every change never delivered, with its key, field and value |
| `error`, `passed` | Why the run stopped early, and whether it passed |

## Running

The drivers read `HASHI_SOAK_SECS` (run length), `HASHI_SOAK_FAULT_SECS` (time
between faults; default one round spread over the run) and
`HASHI_SOAK_REPORT_DIR` (where `<family>.json` is written):

```sh
cargo test --test soak_modbus # default feature modbus
cargo test --features opcua-simulator --test opcua_subscription opcua_survives
bash scripts/test-mqtt-container.sh
bash scripts/test-mysql-container.sh
```

Pull requests run a short soak of every family: Modbus and OPC UA in Build &
Test, MQTT and MySQL at the end of their container jobs, alone, because they
pause and restart the shared fixture. `HASHI_SOAK_ONLY=1` makes a container
script run only the soak.

The nightly `Soak` workflow runs each family for about five hours on hosted
runners, with a fault every ten minutes, and publishes the reports as
artifacts. The 24-hour evidence run is separate (#29).
