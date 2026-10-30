# OPC UA sessions and discovery

Enable the `opcua` Cargo feature to use `opcua::OpcUaConnector::connect(Config)`.
The async-opcua 0.19 client supplies framing, secure channels, certificate
validation, and services. Hashi owns policy selection, bounds, lifecycle, and
the connector contract. The server stack is only for `opcua-simulator` fixtures.

## Configure a session

Supply a stable id, endpoint, application URI, PKI directory, authentication,
object-container roots, nonzero schema revision, node limit, and timeout:

```rust,ignore
let config = Config {
    id: "equipment".into(),
    endpoint,
    application_uri: "urn:hashi:equipment".into(),
    pki_dir,
    policy: Policy::Basic256Sha256,
    allow_insecure_lab: false,
    authentication: Authentication::UserName { user, password },
    roots: vec![opcua::types::ObjectId::ObjectsFolder.into()],
    max_nodes: 1000,
    timeout: Duration::from_secs(10),
    schema_version: 1,
};
```

Obtain credentials from the embedding application's credential custody. X.509
user authentication instead supplies `Authentication::X509 { certificate,
private_key }`; these paths identify the user's certificate and key, separately
from the application's certificate.

Select Basic256Sha256, Aes128Sha256RsaOaep, or Aes256Sha256RsaPss. All require
SignAndEncrypt and an exact endpoint match. None requires the explicit
`allow_insecure_lab` flag; reserve it for disposable fixtures. No policy
fallback or automatic trust is enabled. The selected URL, including its port,
must match the configured URL before the session starts. Original discovery
descriptions are filtered before session construction, without hostname rewrites. Userinfo, queries,
and fragments are refused in endpoint configuration.

The adapter owns a two-thread runtime. Call its synchronous APIs from a
dedicated OS thread when embedding in an async application. Runtime-attached
calls, including runtime blocking-pool tasks, are rejected before IO. `check` reports connected session state.
Drop stops reconnects and performs bounded shutdown. Discovery-only `connect`
returns None from `read`; `subscribe` enables value collection as described below.
History gap-fill remains separate.

## Provision and approve certificates

Provision the application certificate as `own/cert.der` and private key as
`private/private.pem` inside the configured PKI directory. Issue them with your
certificate tooling: the certificate must bind the configured application URI,
use an appropriate RSA key and validity interval, and contain applicable host
names. Restrict private-key permissions to the connector owner. The connector
requires these files and never generates sample keys. The server must also
trust the client's application certificate.

Use a distinct PKI directory per connector instance; an exclusive OS file lock
prevents concurrent mutation. Unknown server certificates are refused and kept
in `rejected/`. After independent fingerprint and scope review, move the
approved certificate from `rejected/` into `trusted/`, keeping the filename
computed by `CertificateStore::cert_file_name`. An arbitrary filename does not
establish trust. A duplicate left in `rejected/` continues to deny the
certificate even if another copy is trusted. Reconnect after approval.
Hostname, application URI, validity, and key checks remain enabled. Connector
diagnostics omit credentials and raw protocol errors.

## Discovery bounds

Roots must be Object containers; variable roots fail explicitly. Expanded
node ids on the current server may use namespace URIs: these resolve through
the session namespace map. References to another server are refused.
Discovery follows hierarchical references and continuation pages, deduplicates
cycles, and enforces total time, message, node count, and browse-path limits.
Any failure or timeout closes the session to release outstanding server
continuations, including tokens hidden by a cancelled response. Reconnect
before retrying discovery.

Each kind has a normalized `value` schema plus descriptive metadata: resolved
node id, browse path, raw data-type id, value rank, and optional engineering
units. Arrays use an opaque byte schema rather than a scalar type. Unknown or
custom scalar types and UInt64 use text. Later readers must preserve the raw
metadata when normalizing those values. A declared but unreadable or malformed
namespace-zero EngineeringUnits property reached through HasProperty fails
discovery. Application-defined children with similar names remain ordinary
nodes. Increment the source revision when
its payload schema changes.

## Simulator conformance

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --locked --features opcua-simulator --test opcua_discovery
bash scripts/test-opcua-container.sh
```

The shared fixture is `examples/support/opcua_simulator.rs`. Each run generates
disposable keys in a unique temporary directory and binds port 0. It checks
rejected-certificate retention and explicit trust, three secure policies,
username and X.509 users, lab-only None, local namespace URI resolution,
metadata, and discovery bounds. No private keys are checked in.

The container harness runs the same client/server fixture with `--network none`,
a unique image tag, and no host ports or state mounts, then removes its image.
This proves protocol behavior, not equipment interoperability.

### Acceptance coverage

The parent connector scope in #3 is delivered by #16, #17, and #18. The same
fixture commands above exercise these capabilities without an external server.

| Capability | Evidence |
| --- | --- |
| Explicit certificate trust, rejected certificates, secure policies, username/X.509 authentication, and lab-only None | [Session/discovery fixture](../tests/opcua_discovery.rs) |
| Typed browse metadata, engineering units, namespace resolution, and discovery bounds | [Session/discovery fixture](../tests/opcua_discovery.rs) |
| Monitored values, sampling/deadband, source timestamps, quality, and durable replay | [Subscription fixture](../tests/opcua_subscription.rs) |
| Recreated subscriptions and historical backfill across server restart, with overlap deduplication | [Historical restart fixture](../tests/opcua_subscription.rs) |
| Unsupported history, unavailable ranges, retained boundaries, bounded receipts, and overflow recovery after restart | [Historical recovery fixture](../tests/opcua_subscription.rs) |
| Throughput and latency for 10,000 monitored items | [Measurement procedure](#10000-item-fixture-measurements) |

These checks establish simulator conformance. Historical retention and append-only
source identity remain requirements of the source described below; this evidence
does not certify arbitrary equipment or historian behavior.

## Durable subscription reads

Use `OpcUaConnector::subscribe(config, SubscriptionConfig { ... })` for value
collection. Supply an explicit, unique list of up to 10,000 scalar `NodeId`s,
publishing and sampling intervals, optional absolute deadband, server queue
size (1–1000), a dedicated `state_dir`, and `max_pending_bytes`. The configured
node set is independent of browse roots. Server authorization still applies.
Numeric namespace indexes must remain stable for this configuration.

The adapter reads type/rank attributes and creates monitored items in batches
of 128. Subscription discovery describes this selected set and caches its
schema for the connection. Arrays are rejected explicitly. A schema change
noticed during subscription recreation fails closed. Restart with a reviewed
schema revision and compatible state recovery when changing the selected set
or its schema; existing spool state must not be discarded under the same
checkpoint identity.

Signed integers, unsigned integers through UInt32, finite floating-point values,
and Booleans become JSON scalars inside `{"value": ...}`. UInt64 and other
scalar types use text, matching the discovered schema without narrowing UInt64
into a signed I64. ByteString values retain their bytes as JSON arrays, with
null distinct from an empty byte sequence. Source timestamps are retained in milliseconds.
Quality includes the symbolic status and its full hexadecimal bits, including
overflow flags. A missing source timestamp, nonfinite numeric value, unknown handle, unexpected
notification, or batch beyond the configured payload bound fails visibly.
Publish timeouts include twice the server-negotiated keepalive period.

Manual Publish requests allow a complete received batch to reach an atomic,
fsynced spool replacement before its server acknowledgement. Subsequent reads
return the same record/order/idempotency identity until the runtime supplies a
committed position. Spool recovery retains uncommitted samples across process
restart and rejects missing or regressed checkpoint state. The spool has its
own exclusive lease; preserve it together with the runtime checkpoint.

Reconnect first retries the existing session's subscription, then attempts transfer
and recreates monitored items when transfer is unavailable. Available unreceipted messages are republished
before accepting newer notifications; recovery is bounded to 64 messages. Sequence continuity includes keepalives
and rollover; an unavailable sequence fails visibly before newer samples are
acknowledged.
A recreated subscription emits current values. Plain subscriptions cannot recover
values changed while a server is unavailable or after its queue overflows.
Use the optional historical access path below when the nodes retain raw history.
Overflow status remains visible.
A failed reader reports unhealthy and must restart to recover durable state.

Reads wait through keepalives until a sample arrives. The embedding runtime
must keep calling read to keep the subscription serviced; server queue and
lifetime limits apply while downstream delivery blocks. This is a single
leased spool on shared durable storage. A standby takes the fencing lease and
resumes from the last checkpoint; values changed while the subscription was
down are recovered only through historical gap fill.

### 10,000-item fixture measurements

`cargo test --locked --features opcua-simulator --test opcua_subscription -- --nocapture`
checks initial values and a second 10,000-node change burst. One debug-build
run measured 1.62 seconds of setup, 19,900 initial records/second, and 16,200
changed records/second. Source-to-read age for the change burst was 425 ms at
p50 and 616 ms at p99. It uses 50 ms publishing, 20 ms requested sampling,
queue size 10, 1024 notifications per response, secure sessions, and fsynced
local receipts. The fixture raises the server's explicit monitored-item limit
to 10,000. These are reproducible burst measurements, not a sustained-capacity
or end-to-end sink guarantee; the command prints fresh measurements per run.

For nonfinite-value fixture cases under absolute deadband, also change status:
a NaN value-difference comparison can suppress a value-only notification. Force
an actual notification before asserting that the reader rejects its value.

## Historical backfill and reconnect gaps

`OpcUaConnector::subscribe_with_history` adds a `HistoryConfig` with an explicit
source start time and page size (1–1024). It creates monitored items first, reads
raw history through the server's current time, and stages that history before
subscription values. History pages use continuation points; the complete recovered
gap must fit `max_pending_bytes` and the recovery deadline. No partial gap is
published if decoding, paging, capacity, or availability fails.

History uses the same leased receipt spool and runtime checkpoint as subscriptions.
The spool atomically persists pending records, watermarks, and overlap identities
before acknowledging notifications. Process restart replays the same positions and
idempotency keys; reconnect reads inclusive history from each node's last durable
source timestamp before resuming its monitored tail. Source timestamp ticks,
picoseconds, status, and normalized value identify overlap. Equal timestamps with
different values remain distinct. Cross-node ordering is deterministic by timestamp
and node; the protocol supplies no transaction order across nodes.

The historical source must retain the requested raw range and use monotonically
ordered source timestamps. Corrections to already emitted historical samples and
indistinguishable repeated samples are outside this append-only contract. Unknown
older values fail visibly. Explicit lost-data markers, unavailable previously
supported history, malformed pages, and unrecoverable mixed-node gaps also fail.
A successful raw read cannot independently prove the server's retention completeness;
configure retention to cover the requested backfill and expected outage window.

A node without advertised history read access, or an initially unsupported history
service, stays on subscription-only capture. `check` remains healthy and reports
the count of unsupported nodes after the first read; the choice is durable for
that source generation. A known notification gap involving such nodes still fails visibly. History mode
checks the whole notification batch for queue overflow before receipt publication;
overflow fails visibly and requires restart, so history starts from the last durable
watermarks rather than from partially accepted values. Changing support policy requires explicit source recovery.
History-enabled reads return `None` on quiet keepalive boundaries so the embedding
runtime can regain control.

The subscription-only fingerprint and spool layout remain compatible. History
mode changes the fingerprint and adds state inside the same atomic spool file;
earlier readers refuse it rather than silently resetting positions. Start history
as a new source generation with a dedicated state directory and compatible consumer
recovery. Preserve both receipt spool and runtime checkpoint on restart. This adds
no cross-node ownership protocol.
