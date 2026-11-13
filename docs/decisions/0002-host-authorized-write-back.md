# 0002. Host-authorized write-back with a fenced ledger

Status: accepted
Source: #98

## Context

Hashi only read from sources. The v0.2 roadmap adds writes to OPC UA and MQTT,
behind approval by the calling host. A write is not a record: it cannot be
replayed from a cursor, and a duplicate can move a plant. The host needs to
tell applied, refused, and unknown apart, and a crash or a takeover must never
send the same command twice.

## Decision

- The host authorizes. Hashi refuses only targets missing from the writer's
  `allow` list, and has no approval logic of its own.
- A `[[writer]]` configuration entry names one target with its own connection
  and credential, separate from read instances. A running OPC UA reader holds
  its PKI store exclusively, and a separate write credential keeps reads
  least-privileged.
- `hashi write` is one-shot, like the outbox. It reads a command file and
  prints a result per command. The library API (`hashi::write`) is the same
  path.
- At most once per command id: before sending, intent is appended to a
  per-writer ledger beside the checkpoints. Appends go through the existing
  checkpoint fence under the writer's lease from the configured lease store. A
  writer admits its token before reading the ledger. Intent without an outcome
  reports `unknown` forever and is never resent.
- Requests are sent once. No client-side retry and no session reconnect while
  a write is in flight.
- Protocols implement one small `Target` (prepare without side effects, send
  once). MQTT (#107) adds a second target on the same ledger.

## Consequences

- A lost response leaves the host to check the target and decide. Hashi never
  guesses.
- The ledger grows without compaction until a later change bounds it.
- A writer and a read instance cannot share a name, because both own
  `checkpoint_dir/<name>`.
