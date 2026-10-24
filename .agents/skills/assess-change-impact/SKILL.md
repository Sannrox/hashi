---
name: assess-change-impact
description: Assess a proposed or implemented hashi change across connector, checkpoint, sink, and operations boundaries. Use when scoping an Issue, planning tests, reviewing a diff, or identifying documentation, compatibility, and security obligations.
---

# Assess Change Impact

Build an evidence-backed impact map before implementation or review.

## Procedure

1. Read the linked Issue or request, `VISION.md`, `README.md`, and the
   relevant code. For a diff, inspect every changed file and its direct
   callers or implementors. Complete when the claimed outcome and actual change
   surface are both known.
2. Trace applicable boundaries:
   - connector check/discover/read versus durable checkpoints;
   - at-least-once delivery versus sink ack and the store-and-forward buffer;
   - schema drift versus quarantine;
   - protocol adapters (Modbus, OPC UA, MQTT, MySQL) versus optional features;
   - configuration, CLI, metrics, diagnostics, and the static image.
   Complete when each applicable boundary has an owner and expected invariant.
3. Identify compatibility obligations. Include old configuration, checkpoint
   and cursor formats, error semantics, and fail-closed paths (missing
   credentials, unreadable checkpoints, sink nack, schema drift). Complete when
   partial-failure paths are accounted for.
4. Map evidence to risk: unit tests for pure logic; integration tests under
   `tests/` for connectors and multi-component behavior; focused tests for
   config parsing. Complete when every material risk has a proposed check or an
   explicit residual uncertainty.
5. Determine durable artifacts that must change: README, `docs/`, an ADR under
   `docs/decisions/`, an Issue comment, or a repository Skill. Complete when no
   artifact is proposed merely to record temporary planning.

## Output

Return a compact matrix with columns:

| Surface | Evidence found | Required change/check | Risk if missed |
| --- | --- | --- | --- |

Then list scope boundaries, blocking questions, and the smallest safe PR split.
Do not approve an architecture, perform a full security audit, or claim
behavior without inspecting the implementations.
