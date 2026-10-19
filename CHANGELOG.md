# Changelog

## Unreleased

- Connector runtime: `check`, `discover`, and `read` against a versioned
  record contract. Checkpoints live in the runtime and commit only after the
  sink acks. Delivery is at-least-once; each record carries an idempotency
  key. Poison records go to a quarantine log. A fake source covers snapshot,
  incremental, kill/resume, and quarantine (#1).
