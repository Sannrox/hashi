# Historian backfill and live tail

`historian::Historian` is the one trait a historian adapter implements: tag
discovery, raw and interpolated time-range reads with paging, and a live tail
(by default the raw read; a subscribing historian serves it from samples it
keeps durably). `historian::HistorianConnector` turns any historian into a
connector.

## Backfill and handover

The connector backfills every tag from `start_ms`, then follows the tail.
Work runs in rounds over a window `(lower, upper]`, tag by tag in name order:

- The first round is the backfill, up to `now - settle_ms` when it opens. It
  reads each tag in `chunk_ms` time chunks, `page_limit` samples per read, at
  most `max_reads_per_second` reads.
- Each later round is a tail round from the previous `upper` to
  `now - settle_ms`. `settle_ms` leaves time for late writes to land.

Each tag reads strictly after its watermark, so the backfill hands over to the
tail with no gap and no overlap.

A round's bounds are written to the connector's `state_dir` (fsynced, then
renamed into place) before any of its records is delivered. A restart reuses
the saved round, so a record re-emitted after a crash has the same content
and position. A round that turns out empty is widened in place to the next
horizon instead of being followed by a new one, so the saved round always
starts where the last checkpointed round ended. The state directory is leased: a second connector on it is
refused.

## Checkpoints

The checkpoint per tag lives in the connector cursor, which the runtime commits
after the sink acknowledges each record. The cursor holds the round, the tag
index, that tag's watermark and a fingerprint of the tag set; that implies
every tag's checkpoint (earlier tags are at `upper`, later ones at `lower`), so
it stays a few bytes for any number of tags. Discovery order does not matter;
a changed tag set or backfill start refuses the old cursor rather than
misapply it. A restart resumes from the committed cursor and re-emits the
same records at the same positions, so a redelivered record keeps its
idempotency key. A cursor from another configuration, or a position without a
cursor, is refused.

## Contract

`tail` must be replayable: called again with the same arguments, for example
after a restart, it returns the same samples. A subscribing adapter keeps
received samples durably until the watermark passes them.

Timestamps of one tag strictly increase; a historian that returns a sample at
or before the watermark, or outside the requested range, is refused. Records
use kind `historian.sample`, key `<tag>@<ts_ms>`, and a JSON payload with
`tag`, `ts_ms` and `value` (a number, or `"NaN"`, `"Infinity"` or
`"-Infinity"` for non-finite readings); the sample quality maps to the record quality.
