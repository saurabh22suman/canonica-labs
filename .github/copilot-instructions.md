# GitHub Copilot Instructions – canonic-labs

## Purpose

This file defines **mandatory rules** for using GitHub Copilot in the canonic-labs codebase.

Copilot is an assistant.
Copilot is **not** an authority.

Human engineers own:
- correctness
- semantics
- safety
- trust

These instructions are inspired by best practices curated in the **awesome GitHub Copilot** repository.

---

## Core Rules (Non-Negotiable)

1. **Copilot may assist, never decide**
2. **Copilot output must be reviewed**
3. **Copilot must never invent behavior**
4. **If unsure, code must fail**

Violations of these rules are considered defects.

---

## Allowed Uses of Copilot

Copilot MAY be used for:

- Boilerplate code
- Struct definitions
- Interface scaffolding
- Table-driven test templates
- Repetitive adapters
- CLI command wiring
- Mock generation

Copilot is especially encouraged for:
- reducing typing
- improving consistency
- speeding up refactors

---

## Forbidden Uses of Copilot

Copilot MUST NOT be used for:

- Planner decision logic
- Capability enforcement rules
- Constraint evaluation
- Security / authorization logic
- Error semantics
- Policy enforcement
- Routing decisions

If Copilot generates any of the above, the code MUST be rewritten by a human.

---

## Red-Flag / Green-Flag TDD Enforcement

All code MUST follow **Red-Flag / Green-Flag TDD** as defined in `docs/test.md`.

### Copilot-Specific Rules

- Copilot may generate test scaffolding
- Copilot may NOT decide test assertions
- Red-Flag tests MUST be written first
- Green-Flag tests MUST be explicit

If Copilot suggests only happy-path tests, reject the suggestion.

---

## Error Handling Instructions

Copilot-generated code MUST:

- return explicit errors
- include human-readable messages
- never swallow errors
- never silently fallback

Example of FORBIDDEN behavior:

```go
if err != nil {
    return nil
}
```

Example of REQUIRED behavior:

```go
if err != nil {
    return fmt.Errorf("query rejected: %w", err)
}
```

---

## SQL and Semantics

Copilot MUST NOT:
- assume SQL dialect behavior
- infer engine capabilities
- guess format semantics

All SQL semantics must be enforced via:
- Table Abstraction Layer
- Capability checks
- Explicit planner rules

---

## Security Rules

Copilot-generated code MUST NOT:
- hardcode credentials
- bypass authorization checks
- weaken validation logic

Security-related code must be:
- minimal
- explicit
- reviewed

---

## Commenting and Documentation

Copilot SHOULD:
- generate comments explaining *what*, not *why*
- avoid speculative comments
- avoid TODOs without context

Humans document intent.
Copilot documents structure.

---

## Tracker.md — Deferred Features & Technical Debt

All deferred features, future improvements, and technical debt MUST be logged in `docs/tracker.md`.

### Rules

- **Never leave TODOs in code** without a corresponding tracker entry
- Every tracker entry MUST include: description, rationale, and priority
- Tracker entries should reference related plan.md sections where applicable
- Before implementing a deferred feature, check tracker.md for context

### Tracker Entry Format

```markdown
### [ID] Feature/Debt Name
- **Status**: Deferred | In-Progress | Blocked
- **Priority**: P0 (Critical) | P1 (High) | P2 (Medium) | P3 (Low)
- **Rationale**: Why this is deferred
- **Context**: Related docs, constraints, or dependencies
```

---

## Tests and Coverage

Copilot MAY:
- expand table-driven test cases
- generate mock data

Copilot MUST NOT:
- remove Red-Flag tests
- weaken assertions
- skip edge cases

Every feature requires:
- ≥1 Red-Flag test
- ≥1 Green-Flag test

---

## Final Rule

> **Copilot accelerates work. Humans enforce truth.**

If there is conflict between:
- Copilot suggestion
- plan.md
- test.md

The documents win.
Always.
