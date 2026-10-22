# Running from a config file

```sh
hashi run --config hashi.toml           # run until stopped, one pass per second
hashi run --config hashi.toml --once    # one pass, exit nonzero if any instance failed
hashi run --config hashi.toml --interval-ms 5000
```

## Config

TOML. Relative paths resolve against the config file's directory.

```toml
checkpoint_dir = "state"

[[instance]]
name = "line-1"
source.fixture = { records = 3, credential_file = "secrets/line-1", accept_sha256 = ["<hex>"] }
sink.jsonl = { dir = "out/line-1" }
```

- `checkpoint_dir`: each instance checkpoints under `checkpoint_dir/<name>`,
  so a restarted process resumes after the last committed position.
- `instance.name`: `A-Z a-z 0-9 - _`, unique.
- `instance.source.<family>`: one section per source family. This release
  ships `fixture`, the in-process source used to prove the runtime; protocol
  families add their own sections.
- `instance.sink.jsonl.dir`: the JSON Lines sink directory. Instances may not
  share one; a running instance also holds an exclusive lock in it
  (`.hashi-sink.lock`), so a second writer that reaches it by another path,
  in this process or another, is refused until the first stops.

Unknown fields are refused, and every error names its field, for example
`invalid config: instance[0].source.fixture.records: must be at least 1`.

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
