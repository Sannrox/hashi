# Running from a config file

```sh
hashi run --config hashi.toml           # run until stopped, one pass per second
hashi run --config hashi.toml --once    # one pass, exit nonzero if any instance failed
hashi run --config hashi.toml --interval-ms 5000
```

Add `--listen <addr>` for health and metrics, and use `hashi diagnostics
--config hashi.toml` for a support bundle; see [operations](operations.md).

## Config

TOML. Relative paths resolve against the config file's directory.

```toml
checkpoint_dir = "state"
# Optional. Default is a file slot under checkpoint_dir.
# lease = { backend = "file", ttl_ms = 15000, members = 1 }

[[instance]]
name = "line-1"
source.fixture = { records = 3, credential_file = "secrets/line-1", accept_sha256 = ["<hex>"] }
sink.jsonl = { dir = "out/line-1" }
```

- `checkpoint_dir`: each instance checkpoints under `checkpoint_dir/<name>`,
  so a restarted process resumes after the last committed position.
- `lease` (optional): the lock store that decides who may write. Checkpoint
  bytes stay in Hashi. `backend` is `file` (default, a JSON slot under
  `lease.dir` or `checkpoint_dir`), `postgres` (URL in `credential_file`,
  needs the `lease-postgres` feature), or `kubernetes` (`namespace`, `api`,
  and a bearer token in `credential_file`, needs `lease-kubernetes`).
  `ttl_ms` is the hold (default 15000). `members` is the site size; two nodes
  need this exclusive lock or a witness, otherwise automatic promotion is
  refused. A standby waits on the lease and resumes from the last checkpoint.
  Hashi has no hard PostgreSQL dependency; a single-box site keeps the file
  backend.
- `instance.name`: `A-Z a-z 0-9 - _`, unique.
- `instance.source.<family>`: one section per source family. This release
  ships `fixture`, the in-process source used to prove the runtime; protocol
  families add their own sections.
- `instance.sink.jsonl.dir`: the JSON Lines sink directory. Instances may not
  share one; a running instance also holds a fencing lease in it, so a second
  writer that reaches it by another path is refused until the first stops or
  its TTL expires. Every sink write carries the instance fencing token, so a
  paused holder is refused after takeover.
- `instance.buffer` (optional): a store-and-forward disk buffer between the
  source and the sink, for sites whose uplink drops. See
  [Store-and-forward buffer](#store-and-forward-buffer).

Unknown fields are refused, and every error names its field, for example
`invalid config: instance[0].source.fixture.records: must be at least 1`.

## Store-and-forward buffer

```toml
buffer = { dir = "buffer/line-1", max_bytes = 1073741824, max_age_secs = 86400, when_full = "stop" }
```

A record is acknowledged, and the checkpoint advances, once it is durably
written to the buffer. The buffer drains to the sink in order, with each
record's original idempotency key, whenever the sink accepts again. It is
local files under `dir`, with no lock database, and a running instance holds
a fencing lease there. The directory may not be a sink's or another buffer's.

- `max_bytes`: the most buffered data.
- `max_age_secs` (optional): the oldest buffered record, by its ingest time,
  may be at most this old.
- `when_full`: `stop` (default) refuses the next record, so the source stops
  advancing until the buffer drains. `drop_oldest` drops the oldest records
  to make room. Each dropped record's idempotency key and ingest time go to
  `gaps.log` in `dir` (no payload; rotated at 1 MiB into `gaps.1.log`), and
  its total is kept in `dropped` and reported as
  `hashi_buffer_dropped_total`. A schema control is never dropped; a full
  buffer behind one stops.

The sink opens, and takes its lock, on first delivery, so an instance whose
sink cannot open yet still starts and buffers. A crash while draining
redelivers from the last stored cursor with the same keys, so nothing is
lost. Delivery is at least once: the JSON Lines sink appends every delivery,
so consumers deduplicate by idempotency key.

## Credentials

A config names credentials by file path, never by value. The file is read on
every use, so writing a new value rotates the credential on the next pass
without a restart. Values never appear in logs or errors; a missing or empty
file is reported by path only. The `fixture` source accepts a credential when
the SHA-256 of the file's trimmed content is listed in `accept_sha256`, so the
config never holds the value.

Each pass runs every instance until its source is caught up. One instance's
failure is logged and does not stop the others; an instance that cannot open
its directories yet is retried on the next pass.
