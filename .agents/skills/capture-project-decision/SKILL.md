---
name: capture-project-decision
description: Capture an accepted hashi design or project outcome in the smallest durable artifact. Use after a Discussion, Issue, or PR resolves a choice that may require an ADR, maintained documentation, an example, or a reusable Skill.
---

# Capture Project Decision

Promote the durable result, not the conversation that produced it.

## Procedure

1. Read the complete source Discussion, Issue, PR, and relevant current docs or
   code. Confirm the choice is accepted rather than merely proposed. Complete
   when the decision, owner, alternatives, and supporting evidence are named.
2. Select exactly the artifacts justified by future use:
   - ADR: a durable boundary, public contract, persistence strategy, or
     difficult-to-reverse technical choice (`docs/decisions/`);
   - documentation: repeated operator, integrator, or contributor knowledge
     (`README.md`, `VISION.md`, `docs/`);
   - example: a supported path is best taught and tested as executable code;
   - Skill: a repeated project-specific AI procedure or high-risk checklist;
   - none: the outcome is local to the closed work item.
   Complete when every artifact has a future audience and owner.
3. Preserve one source of truth. Link to code rather than duplicating
   exhaustive details. Put rationale in the ADR and usage in docs. Complete when
   the same rule is not maintained in multiple prose locations.
4. For an ADR, add the next numbered file under `docs/decisions/` following
   `0001-single-package-crate-graph.md`, include the source links, and update
   `docs/decisions/README.md`. There is no `0000-template.md`. Supersede by
   adding a new ADR and cross-linking both; retain historical text. Complete
   when status and relationships are explicit.
5. Check links, examples, Skill metadata, and affected indexes. Complete when a
   future contributor can discover the result without the original prompt.

## Output

State the selected artifact(s), why each is durable, files changed or proposed,
source links, owner, and validation. If no promotion is warranted, say so and
leave the knowledge in the closed Issue/PR.

Do not turn unresolved debate into policy, copy investigation logs into docs,
or create an ADR for routine implementation detail.
