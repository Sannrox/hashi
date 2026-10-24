# Connector conformance kit

`hashi::conformance::run` proves that a source family keeps the connector
contract. It drives the family's connector through the real runtime and a
durable checkpoint store, against a live fixture, and checks in order:

| Check | What must hold |
| --- | --- |
| `check` | `check` reports ok. |
| `discover` | Discovery is not empty; every kind is valid, unique and not reserved. |
| `snapshot` | State seeded before the first connect and changes made after it are delivered, with positions that only grow. |
| `incremental` | A later change is delivered after the committed checkpoint, never at or before it. |
| `interruption_and_resume` | After a kill right after the first delivery, a fresh connector delivers every remaining change and resumes after the durable checkpoint. |
| `duplicate_keys` | Two changes to one key are two records with distinct identities; neither is dropped. |
| `drift` | A schema change produces a schema control record for the kind and the following change is delivered, or, for a family that fails closed, a refused start that delivers and commits nothing. |

The first violation fails the run and names its check. A watchdog ends the
process after ten minutes, naming the stuck check, because a connector read can
block longer than the kit's per-drain deadline. The durable checkpoint
must match the last committed position after every drain.

## Families

A family implements `conformance::Family`: `connect` a connector over the
fixture's current state, `produce` source changes and return the records they
must yield, and `drift` the schema. An optional `seed` creates state before the
first connect; MySQL seeds rows so its snapshot path is proven. Each expected record names its key and a
payload field holding the value the change wrote, so an older record for the
same key (an initial sample, a backlog) never stands in for a new change. A sampling source such as Modbus may
return one key per change set, since it reports current state.

| Family | Fixture | Drift | Runs in CI |
| --- | --- | --- | --- |
| Modbus | In-process register simulator | Register-map revision after acknowledging the spool | Build & Test |
| OPC UA | In-process simulator | Refused: a schema revision on an existing spool fails closed ([opcua.md](opcua.md)) | Build & Test, OPC UA container fixture |
| MQTT | Disposable broker | Schema revision | MQTT container fixture |
| MySQL | Disposable database | `ALTER TABLE ... ADD COLUMN` | MySQL container fixture |

Run one family locally with `cargo test --test conformance_modbus` (default feature `modbus`),
`cargo test --features opcua-simulator --test opcua_subscription opcua_passes`,
or the family's container script (`scripts/test-mqtt-container.sh`,
`scripts/test-mysql-container.sh`). The MySQL script runs its tests one at a
time because they share one disposable server.
