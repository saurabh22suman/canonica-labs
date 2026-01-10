# Phase 4 Implementation – Make the System Operable and Enforce Closure

## Status
**Authoritative**
This document defines the mandatory implementation steps required to:
- close Phase 3 findings
- make the system operationally truthful
- prepare canonic-labs for real users and demos

Phase 4 is **execution-focused**.
No new concepts are introduced.

---

## Phase 4 Objectives

Phase 4 exists to resolve the following confirmed issues:

1. PostgreSQL authority exists but is not enforced at runtime
2. CLI is structurally present but not yet a true gateway client
3. Invariants are tested but not structurally guaranteed
4. Operators lack clear failure signals and readiness checks

After Phase 4:
> If the system starts, it is real.  
> If it runs, it is authoritative.

---

## Mandatory Implementation Tasks

### 1. Enforce PostgreSQL as the Only Metadata Authority

#### Objective
Remove all ambiguity around metadata sources.

#### Required Actions

1. **Delete in-memory registry fallback**
   - Remove any default or implicit in-memory registry creation
   - Production code paths MUST NOT create registries implicitly

2. **Make PostgreSQL dependency explicit**
   - Gateway constructor MUST require a repository instance
   - No zero-value or default repository allowed

3. **Fail Fast on Startup**
   - If PostgreSQL is unreachable at startup:
     - gateway MUST NOT start
     - error must clearly state database dependency failure

#### Required Code Changes

- Refactor gateway initialization:
  ```
  NewGateway(repo Repository) → error
  ```

- Remove:
  - `NewGatewayWithDefaultRegistry`
  - any `if repo == nil` logic

#### Red-Flag Tests (MANDATORY)

- Gateway startup without PostgreSQL connection
- Repository injection omitted
- Attempt to read metadata before repository initialization

All must fail deterministically.

---

### 2. Wire PostgreSQL into All Control Paths

#### Objective
Ensure metadata persistence is not optional.

#### Required Actions

- Route ALL of the following through PostgreSQL:
  - table registration
  - table lookup
  - capability resolution
  - authorization checks
  - engine discovery

- Remove duplicated logic across repositories

#### Validation

- Restart gateway → metadata persists
- Concurrent requests observe consistent metadata

#### Green-Flag Tests

- Table registered, gateway restarted, table still visible
- Authorization rules survive restart

---

### 3. Convert CLI into a Pure Gateway Client

#### Objective
Eliminate local simulation.

#### Required Actions

1. **Delete local CLI logic**
   - Remove any code that:
     - reads local files for metadata
     - performs planning or validation locally

2. **Enforce Gateway Dependency**
   - All CLI commands MUST:
     - authenticate
     - call gateway HTTP APIs
     - render responses verbatim

3. **Fail on Gateway Unavailability**
   - No offline mode
   - No degraded mode

#### Required Structural Changes

- Introduce:
  ```
  GatewayClient (required)
  ```

- Each CLI command must:
  - construct GatewayClient
  - execute exactly one API call

#### Red-Flag Tests

- CLI run without gateway → must fail
- CLI output diverges from gateway response → must fail

---

### 4. Add Explicit Readiness and Liveness Checks

#### Objective
Make operational state observable.

#### Required Endpoints

- `/healthz`
  - reports process health only

- `/readyz`
  - reports:
    - PostgreSQL connectivity
    - engine adapter availability
    - metadata load success

Gateway MUST:
- refuse queries if not ready

#### Red-Flag Tests

- Database down → readyz fails
- Engine unavailable → readyz fails

---

### 5. Add Minimal Operational Logging

#### Objective
Make failures diagnosable without reading code.

#### Required Logs (Structured)

Every request MUST log:
- request_id
- user / role
- tables referenced
- authorization decision
- planner decision
- engine selected
- outcome

Failures MUST log:
- explicit reason
- invariant violated

Silent failures are forbidden.

---

## Explicitly Forbidden in Phase 4

- New features
- New engines
- SQL rewrites
- Performance optimizations
- UI / GUI work

Any PR introducing these must be rejected.

---

## Acceptance Criteria

Phase 4 is complete only if:

1. Gateway cannot start without PostgreSQL
2. CLI cannot operate without gateway
3. Metadata survives restarts
4. Readiness reflects real operability
5. All Phase 4 Red-Flag tests fail before implementation and pass after

---

## Operator Contract (New)

After Phase 4:

- If gateway is running → metadata is durable
- If CLI responds → gateway state is authoritative
- If readyz is green → system can safely accept queries

Anything else is a bug.

---

## Final Statement

Phase 4 is where the system stops being “correct in theory”
and becomes **safe to run**.

Operational truth is the last invariant.
