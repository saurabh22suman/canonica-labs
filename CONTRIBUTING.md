# Contributing to canonic-labs

Thank you for your interest in contributing to **canonic-labs**.

This project builds **infrastructure-grade control-plane software**.
That requires discipline, clarity, and a shared understanding of responsibility.

This document defines **how to contribute**, **what is expected**, and **what will be rejected**.

---

## 1. Core Philosophy (Read First)

canonic-labs is:
- correctness-first
- semantics-driven
- control-plane only

We value:
- refusal over guesswork
- explicit failure over silent success
- boring, predictable systems

If this philosophy does not resonate, this may not be the right project for you.

---

## 2. Required Reading (Mandatory)

Before submitting **any** contribution, you must read:

1. `docs/plan.md`  
   → Architecture, scope, non-negotiables

2. `docs/test.md`  
   → Red-Flag / Green-Flag TDD doctrine

3. `.github/copilot-instructions.md`  
   → AI and Copilot usage rules

4. `docs/skills.md`  
   → Human skill expectations

5. `docs/agents.md`  
   → Approved AI agent roles (Opus 4.5)

Pull requests that violate these documents will be closed.

---

## 3. Contribution Types

We accept the following contribution types:

### ✅ Accepted
- Bug fixes with Red-Flag coverage
- New capabilities with explicit constraints
- Documentation improvements
- Test coverage improvements
- Refactors that preserve behavior

### ❌ Rejected
- Feature-only PRs without tests
- Optimizations without correctness proof
- Behavior changes without discussion
- Silent fallback logic
- AI-generated logic without human review

---

## 4. Development Workflow (Mandatory)

All contributions must follow this order:

1. Identify the invariant
2. Write **Red-Flag test(s)**
3. Write **Green-Flag test(s)**
4. Implement minimal code
5. All tests pass
6. Open PR

Skipping steps is a process violation.

---

## 5. Red-Flag / Green-Flag Enforcement

Every PR must include:

- At least one Red-Flag test
- At least one Green-Flag test

Red-Flag tests must:
- fail before implementation
- prove unsafe behavior is blocked

Green-Flag tests must:
- demonstrate allowed behavior
- be deterministic

PRs without both will be rejected.

---

## 6. AI Usage Rules (Strict)

We support AI-assisted development using **Opus 4.5**.

However:

### Allowed
- Boilerplate generation
- Test scaffolding
- Documentation drafting
- Mechanical refactors

### Forbidden
- Planner logic
- Capability enforcement
- Constraint evaluation
- Security logic
- Policy decisions

Humans own correctness.
AI accelerates typing.

Violations will block merges.

---

## 7. Code Review Standards

Reviewers will check for:

- Explicit failure modes
- Clear error messages
- Deterministic behavior
- Test intent clarity
- Adherence to plan.md scope

Code that “works” but violates principles will not be merged.

---

## 8. Commit & PR Guidelines

### Commits
- Small, focused commits
- Descriptive messages
- No unrelated changes

### Pull Requests
- Reference the invariant being enforced
- Reference added Red-Flag tests
- Explain *why* the change is safe

PR templates must be followed.

---

## 9. CI Expectations

CI will block PRs that:

- Reduce Red-Flag coverage
- Skip tests
- Introduce silent failures
- Violate linting or formatting

CI failures are not negotiable.

---

## 10. When in Doubt

If you are unsure:
- block the operation
- fail the query
- ask for review

Correctness beats convenience.
Always.

---

## 11. Final Statement

By contributing, you agree that:

- trust is earned through refusal
- correctness is a feature
- boring systems scale best

Thank you for helping build canonic-labs the right way.
