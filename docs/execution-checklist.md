# CANONIC-LABS — SINGLE EXECUTION CHECKLIST

This checklist is **binding**.
If an item is unchecked, the system is **not done**, regardless of code quality.

---

## RULE ZERO (NON-NEGOTIABLE)

☐ **Phase order is enforced**  
No work on Phase N+1 until Phase N exit criteria are met.

☐ **Integration > Components**  
A task is incomplete unless the production execution path uses it.

☐ **Deletion is expected**  
If nothing was deleted, assume the work is incomplete.

---

## PHASE 4 — INTEGRATION ENFORCEMENT  
**Goal: The system must be incapable of lying**

### 4.1 Metadata Authority (P0)

☐ `InMemoryTableRegistry` removed from all production code  
☐ No fallback to in-memory metadata anywhere  
☐ Gateway startup fails if PostgreSQL is unavailable  
☐ Planner reads tables, roles, constraints **only** from repository  
☐ Repository is mandatory in gateway constructor  

**Red-Flag Tests**
☐ Planner bypassing repository fails  
☐ Gateway starts without Postgres → FAILS  

**Stop Check**
☐ `grep InMemoryTableRegistry` returns **tests only**

---

### 4.2 CLI Truthfulness (P1)

☐ All local CLI logic deleted  
☐ CLI commands use `GatewayClient` exclusively  
☐ CLI performs exactly one HTTP call per command  
☐ CLI fails hard if gateway is unreachable  
☐ No local planning, validation, or defaults  

**Red-Flag Tests**
☐ CLI works without gateway → FAILS  
☐ CLI output diverges from gateway → IMPOSSIBLE  

**Stop Check**
☐ CLI cannot run in isolation

---

### 4.3 Engine Wiring Reality (P1)

☐ Trino adapter registered in AdapterRegistry  
☐ Adapter init errors propagate (no log-and-continue)  
☐ Gateway startup fails if adapter registry is empty  
☐ `/readyz` reports adapter availability  

**Red-Flag Tests**
☐ Gateway starts with zero adapters → FAILS  
☐ Trino configured but not selectable → FAILS  

**Stop Check**
☐ `EXPLAIN CANONIC` can select Trino

---

### 4.4 Startup & Migration Enforcement (P2)

☐ Migration runner integrated  
☐ Migrations run automatically on startup  
☐ Gateway fails startup on migration failure  

**Red-Flag Tests**
☐ Fresh DB + gateway → WORKS  
☐ Broken migration → STARTUP FAILS  

---

### ✅ PHASE 4 EXIT GATE

☐ No silent fallbacks exist  
☐ No local CLI logic exists  
☐ No non-authoritative metadata paths exist  
☐ System cannot “start and fail later”

---

## PHASE 5 — ADOPTION & ERGONOMICS  
**Goal: Usable without weakening invariants**

### 5.1 Canonical Configuration

☐ Exactly one config system exists  
☐ Other config loaders deleted  
☐ Strict schema validation enabled  
☐ Unknown config keys fail validation  

**Red-Flag Tests**
☐ Partial config passes → FAILS  
☐ Unknown fields pass → FAILS  

---

### 5.2 Bootstrap Lifecycle

☐ `canonic bootstrap init` implemented  
☐ `canonic bootstrap validate` implemented  
☐ `canonic bootstrap apply` implemented  
☐ Apply is idempotent  
☐ Destructive changes require explicit confirmation  

**Red-Flag Tests**
☐ Apply without validate → FAILS  
☐ Unsafe change without confirmation → FAILS  

---

### 5.3 EXPLAIN CANONIC

☐ `EXPLAIN CANONIC` syntax supported  
☐ Output includes:
- tables  
- capabilities  
- authorization result  
- snapshot requirements  
- engine decision  
- refusal reason  

☐ Explain output is deterministic  

**Red-Flag Tests**
☐ Explain succeeds but execution fails → FORBIDDEN  
☐ Explain hides auth failure → FORBIDDEN  

---

### 5.4 Operator Insight (No UI)

☐ `canonic status` implemented  
☐ `canonic audit summary` implemented  
☐ Status reflects `/readyz` truth  
☐ Audit exposes summaries only (no raw data)  

**Red-Flag Tests**
☐ Status reports healthy when system isn’t → FAILS  
☐ Audit leaks sensitive data → FAILS  

---

### ✅ PHASE 5 EXIT GATE

☐ Fresh install possible without reading code  
☐ System explains every accept/refuse decision  
☐ Operators can assess health without logs  

---

## PHASE 6 — OBSERVABILITY DEPTH (OPTIONAL)

☐ Metrics for:
- accepted queries  
- rejected queries  
- rejection reasons  
- per-engine routing  
- auth failures  

☐ “Why was this blocked?” answerable in <10 seconds  

---

## PHASE 7 — INTEGRATION SURFACES (ONLY IF ASKED)

☐ JDBC read-only (no rewrites, no dialect magic)  
☐ Terraform provider for tables, roles, engines  

---

## PHASE 8 — EXPLICIT NON-GOALS (DO NOT TOUCH)

☐ UI / GUI  
☐ Writes  
☐ Streaming  
☐ Cross-engine joins  
☐ Cost-based optimization  
☐ AI query rewriting  

---

## FINAL SIGN-OFF (MANDATORY)

☐ Every completed task has:
- integration coverage  
- deletion evidence  
- no fallback paths  

☐ No task marked “done” based on file existence  
☐ No phase skipped  

---

## ENFORCEMENT STATEMENT

> **A feature is not done until it is the only possible execution path.**
