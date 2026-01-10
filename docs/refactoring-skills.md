# refactoring-skills.md – AI-Assisted Refactoring Guidelines for canonic-labs

## Purpose

This document defines **refactoring practices** when using GitHub Copilot and Opus 4.5 in canonic-labs.

Refactoring is a discipline.
AI tools **accelerate** refactoring but do not **authorize** it.

Human engineers verify:
- behavior preservation
- semantic correctness
- test validity

---

## Core Principles (Non-Negotiable)

1. **Refactoring must not change behavior**
2. **Tests must pass before AND after**
3. **AI suggestions must be reviewed**
4. **Mechanical refactors only**

If behavior changes, it is NOT refactoring—it is a feature change.

---

## Copilot's Refactoring Strengths

Use Copilot for:

- **Renaming** – variables, functions, types
- **Extracting** – helpers, interfaces, constants
- **Inlining** – removing unnecessary abstractions
- **Moving** – relocating code to appropriate packages
- **Simplifying** – reducing complexity without changing logic
- **Formatting** – consistent code style

Copilot excels at:
- Pattern recognition across files
- Generating repetitive transformations
- Suggesting idiomatic Go patterns
- Completing mechanical edits

---

## Copilot's Refactoring Weaknesses

**DO NOT** use Copilot for:

- **Behavioral changes** – Copilot may silently alter logic
- **Complex merges** – AI struggles with dependency chains
- **Security refactors** – authorization logic requires human review
- **Planner/Router logic** – forbidden per copilot-instructions.md
- **Error semantic changes** – error meaning must be preserved

Copilot is **not designed** to:
- Verify correctness
- Understand system invariants
- Preserve semantic intent

---

## Refactoring Workflow (Mandatory)

### Step 1: Verify Green Tests
```bash
go test ./... -v
```
All tests MUST pass before refactoring begins.

### Step 2: Define Refactoring Scope
Document what you are changing:
- Which files?
- Which functions/types?
- What transformation?

Scope creep is a defect.

### Step 3: Use Copilot for Mechanical Changes
Allow Copilot to assist with:
- Renaming across files
- Extracting repeated patterns
- Generating updated test scaffolds

### Step 4: Review Every Suggestion
Before accepting ANY Copilot suggestion:
- Read the code
- Verify behavior preservation
- Check error handling

Copilot output is **untrusted** by default.

### Step 5: Run Tests After Each Change
```bash
go test ./... -v
```
Tests must pass after EVERY refactoring step.

### Step 6: Commit Atomically
Each commit should represent ONE logical refactoring.
Avoid mega-commits.

---

## Prompt Engineering for Refactoring

### Effective Prompts

**Good:**
```
Rename the function `processQuery` to `executeQuery` in internal/gateway/gateway.go
and update all call sites.
```

**Good:**
```
Extract the error handling logic from lines 45-52 into a helper function
called `handleQueryError`.
```

**Good:**
```
Move the `EngineAdapter` interface from internal/adapters/duckdb/adapter.go
to internal/adapters/adapter.go.
```

### Ineffective Prompts

**Bad:**
```
Make this code better.
```

**Bad:**
```
Refactor this entire file.
```

**Bad:**
```
Optimize the query handling.
```

These prompts lack specificity and invite behavioral changes.

---

## Refactoring Patterns (Approved)

### 1. Extract Interface
**When:** Multiple types share behavior
**Copilot use:** Generate interface definition
**Human responsibility:** Verify semantic correctness

### 2. Rename Symbol
**When:** Name no longer reflects purpose
**Copilot use:** Find and replace across files
**Human responsibility:** Ensure no shadowing or conflicts

### 3. Extract Function
**When:** Code block is reused or too complex
**Copilot use:** Generate function signature and body
**Human responsibility:** Verify no side-effect changes

### 4. Inline Function
**When:** Abstraction adds no value
**Copilot use:** Replace call sites with implementation
**Human responsibility:** Verify no behavioral changes

### 5. Move to Package
**When:** Code belongs in different module
**Copilot use:** Update imports and references
**Human responsibility:** Verify visibility and dependencies

### 6. Simplify Conditionals
**When:** Logic is unnecessarily complex
**Copilot use:** Suggest simplified expressions
**Human responsibility:** Verify truth table equivalence

---

## Anti-Patterns (Forbidden)

### 1. Refactor Without Tests
**Problem:** No way to verify behavior preservation
**Rule:** Red-Flag + Green-Flag tests required before refactoring

### 2. Accept All Suggestions Blindly
**Problem:** Copilot may introduce subtle bugs
**Rule:** Review every line before accepting

### 3. Combine Refactoring with Feature Work
**Problem:** Impossible to verify behavior preservation
**Rule:** Separate commits for refactoring and features

### 4. Refactor Critical Logic with AI
**Problem:** Planner, capability, routing logic requires human precision
**Rule:** Forbidden per copilot-instructions.md

### 5. Skip Post-Refactor Testing
**Problem:** Silent regressions
**Rule:** Run full test suite after every refactoring session

---

## Copilot Context Management

### Provide Good Context

**DO:**
- Open relevant files in editor
- Close irrelevant files
- Use specific file references in prompts
- Break complex refactors into small steps

**DON'T:**
- Have 50 tabs open
- Ask about code Copilot can't see
- Request changes across unrelated modules

### Use Keywords Effectively

```
/explain – Understand code before refactoring
/tests – Generate test scaffolds for refactored code
/fix – Address specific issues without over-refactoring
```

---

## Validation Checklist

Before merging any refactoring PR:

- [ ] All tests pass (`go test ./...`)
- [ ] No new linter warnings
- [ ] Commit messages describe refactoring intent
- [ ] No behavioral changes (only structural)
- [ ] Critical logic unchanged (planner, capabilities, routing)
- [ ] Error semantics preserved
- [ ] Reviewed by human engineer

---

## Integration with Project Documents

This document works with:

| Document | Relationship |
|----------|-------------|
| `copilot-instructions.md` | Defines forbidden Copilot uses |
| `agents.md` | Defines approved agent roles |
| `skills.md` | Defines required human skills |
| `docs/test.md` | Defines TDD requirements |
| `docs/tracker.md` | Logs deferred refactoring work |

---

## Final Rule

> **Refactoring accelerates delivery.**  
> **Correctness enables refactoring.**

If there is conflict between:
- AI suggestion
- Test results
- plan.md requirements

**Tests and documents win. Always.**

Refactoring is a privilege earned by comprehensive tests.
