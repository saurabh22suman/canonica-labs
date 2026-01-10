# Phase 5 Specification – Adoption & Ergonomics

## Status
**Authoritative**
This document is binding for Phase 5 work.

Phase 5 exists to make canonic-labs **usable by real humans** without weakening any invariant
established in Phases 1–4.

This phase optimizes for:
- adoption
- clarity
- operator confidence

It does **not** optimize for performance or features.

---

## Phase 5 Goals

Phase 5 addresses the following problems:

1. System is correct but difficult to configure
2. Fresh installs lack a clear bootstrap path
3. Users cannot easily understand *why* a query is accepted or rejected
4. Operators lack high-level insight without reading logs

After Phase 5:
> A new user can install, configure, and safely use canonic-labs
> without reading the source code.

---

## In-Scope Work (MANDATORY)

### 1. Canonical Configuration System

#### Objective
Introduce a **single, explicit configuration model** that defines system state declaratively.

Configuration must be:
- human-readable
- versionable
- GitOps-friendly
- schema-validated

---

#### Configuration Format (MANDATED)

- YAML
- Single root file or directory
- Explicit sections

Example:
```yaml
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  trino:
    endpoint: http://localhost:8080
    capabilities:
      - READ
      - TIME_TRAVEL

roles:
  analyst:
    tables:
      analytics.sales_orders:
        - READ
        - TIME_TRAVEL

tables:
  analytics.sales_orders:
    sources:
      - engine: trino
        format: iceberg
        location: s3://warehouse/sales_orders
    constraints:
      - SNAPSHOT_CONSISTENT
```

---

#### Enforcement Rules

- Invalid configuration MUST fail validation
- Partial configuration MUST fail
- Unknown fields MUST fail

There is no permissive mode.

---

#### Red-Flag Tests (Required)

- Missing required section
- Unknown configuration keys
- Invalid capability names
- Table without schema-qualified name

---

#### Green-Flag Tests (Required)

- Valid full configuration loads successfully
- Configuration round-trips without loss
- Restart preserves configured state

---

### 2. Bootstrap & Migration Commands

#### Objective
Provide a **safe, explicit install path** for new environments.

---

#### Required CLI Commands

```
canonic bootstrap init
canonic bootstrap validate
canonic bootstrap apply
```

---

#### Command Semantics

##### `bootstrap init`
- Generates example configuration
- Does NOT modify system state

##### `bootstrap validate`
- Validates configuration against schema
- Performs dry-run invariant checks
- Fails on ambiguity

##### `bootstrap apply`
- Applies configuration to PostgreSQL
- Is idempotent
- Refuses destructive changes unless explicitly acknowledged

---

#### Failure Behavior

- Any invariant violation MUST block apply
- Errors must explain:
  - what failed
  - why
  - how to fix

---

#### Red-Flag Tests (Required)

- Apply without validate
- Apply invalid configuration
- Destructive change without confirmation

---

#### Green-Flag Tests (Required)

- Clean install succeeds
- Re-apply is no-op
- Partial updates apply safely

---

### 3. EXPLAIN CANONIC (First-Class)

#### Objective
Make system decisions **inspectable and predictable**.

---

#### Syntax

```sql
EXPLAIN CANONIC
SELECT ...
```

---

#### Required Output Sections

- Tables referenced
- Required capabilities
- Authorization result
- Snapshot requirements
- Engine selection
- Acceptance or refusal reason

---

#### Guarantees

- Output must be deterministic
- Output must match actual execution behavior
- Refusal reasons must be identical to runtime failures

---

#### Red-Flag Tests (Required)

- Explain succeeds but execution fails → forbidden
- Explain hides authorization failure → forbidden

---

#### Green-Flag Tests (Required)

- Explain matches execution routing
- Explain surfaces refusal correctly

---

### 4. Minimal Operator Insight

#### Objective
Provide **high-signal visibility** without dashboards.

---

#### Required CLI Commands

```
canonic status
canonic audit summary
```

---

#### `canonic status`
Displays:
- gateway readiness
- repository health
- engine availability
- active configuration version

---

#### `canonic audit summary`
Displays:
- accepted vs rejected query counts
- top rejection reasons
- top queried tables

No raw data exposure.

---

#### Red-Flag Tests (Required)

- Status reports healthy when system is not ready
- Audit exposes sensitive data

---

#### Green-Flag Tests (Required)

- Status reflects readiness endpoints
- Audit summaries match logs

---

## Out-of-Scope (Explicit)

Phase 5 does NOT include:
- GUI / UI
- JDBC driver
- Terraform provider
- Cost-based optimization
- Query rewriting
- Write support

Any PR adding the above must be rejected.

---

## Acceptance Criteria

Phase 5 is complete only if:

1. System can be installed from scratch via CLI
2. Configuration is validated and enforced
3. EXPLAIN CANONIC is truthful and deterministic
4. Operators can assess health without logs
5. All Phase 5 Red-Flag tests fail before implementation and pass after

---

## Failure Policy

If the system cannot clearly explain itself:
**It MUST refuse to act.**

---

## Final Statement

Phase 5 transforms canonic-labs from
a *correct system* into a **usable platform**.

Adoption without erosion of truth
is the only acceptable outcome.
