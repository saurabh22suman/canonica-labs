# agents.md – AI Agent Roles for canonic-labs

## Purpose

This document defines **approved AI agent roles** when using Opus 4.5 with canonic-labs.

Agents are scoped tools.
They are **not autonomous decision-makers**.

---

## Global Rules (Apply to All Agents)

1. Agents do not own correctness
2. Agents do not approve logic
3. Agents do not bypass Red-Flag tests
4. Agents do not invent requirements

If an agent is unsure, it must say so.

---

## Approved Agents

### 1. Code Scaffolding Agent

**Purpose**
- Generate boilerplate
- Expand repetitive structures
- Wire interfaces

**Allowed**
- Go struct definitions
- CLI command wiring
- Adapter skeletons

**Forbidden**
- Planner logic
- Capability enforcement
- Routing decisions

---

### 2. Test Scaffold Agent

**Purpose**
- Generate test templates
- Expand table-driven test cases

**Allowed**
- Test file structure
- Mock setup
- Repetitive assertions

**Forbidden**
- Deciding pass/fail conditions
- Defining semantic expectations

Humans define assertions.

---

### 3. Documentation Drafting Agent

**Purpose**
- Draft markdown files
- Improve clarity
- Enforce consistency

**Allowed**
- README drafts
- CLI docs
- Internal design docs

**Forbidden**
- Architectural decisions
- Behavior guarantees

---

### 4. Refactor Assistant Agent

**Purpose**
- Suggest mechanical refactors
- Improve readability
- Reduce duplication

**Allowed**
- Renaming
- File reorganization
- Extracting helpers

**Forbidden**
- Changing behavior
- Altering invariants

---

## Opus 4.5-Specific Guidance

When using **Opus 4.5**:

- Prefer explicit instructions over open-ended prompts
- Ask for multiple options, not a single answer
- Require explanations alongside code
- Reject outputs that sound confident but unverifiable

Opus excels at:
- synthesis
- structure
- articulation

Opus must be constrained for:
- correctness
- safety
- semantics

---

## Disallowed Agent Patterns

- “Autonomous coding agents”
- “Fix all failing tests” prompts
- “Optimize without constraints” prompts

These violate control-plane discipline.

---

## Final Rule

> AI agents accelerate work.
> Humans enforce invariants.

If an agent output conflicts with:
- plan.md
- test.md
- copilot-instructions.md

The documents win.
Always.
