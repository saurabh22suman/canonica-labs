# Phase 2 Specification – Make the System Safe for Multiple Users

## Status
**Authoritative**
This document is binding for Phase 2 work.

Phase 2 exists to introduce **explicit authorization semantics** while preserving the control-plane philosophy.
After Phase 2, canonic-labs must be safe to use in **multi-user environments** without relying on trust-by-convention.

---

## Phase 2 Goals

Phase 2 addresses the following gaps:

1. No role → table authorization mapping exists
2. Any authenticated user can access any virtual table
3. Table name resolution rules are implicit and ambiguous

Phase 2 explicitly does **not** introduce fine-grained governance.
It establishes a *minimal, correct, deny-by-default* authorization model.

---

## In-Scope Work (MANDATORY)

### 4. Add Minimal Role → Table Mapping (Deny by Default)

#### Objective
Introduce explicit authorization so that **no user can query any table unless permitted**.

#### Core Principle
> Absence of permission is denial.

There is no implicit access.
There is no wildcard access.
There is no inheritance unless declared.

---

#### Authorization Model (MVP)

##### Entities
- **User / Service Account**
- **Role**
- **Virtual Table**
- **Capability** (READ, TIME_TRAVEL, etc.)

##### Mapping
```
Role → Virtual Table → Allowed Capabilities
```

Example:
```yaml
role: analyst
tables:
  analytics.sales_orders:
    - READ
    - TIME_TRAVEL
```

If a capability is not listed, it is forbidden.

---

#### Enforcement Rules

- Authorization checks occur **before** planning and routing
- Authorization is evaluated per table referenced in a query
- A query referencing multiple tables requires authorization on **all** tables
- Partial authorization is not allowed

If any table fails authorization:
- the query MUST be rejected
- no engine interaction may occur

---

#### Failure Behavior

Errors must:
- clearly state the unauthorized table
- identify the missing capability
- never reveal schema or metadata beyond the table name

Example:
```
Access denied.
Role 'analyst' lacks READ permission on analytics.payments
```

---

#### Red-Flag Tests (Required)

- User with no roles attempts to query a table
- User with role but missing table permission
- User with table permission but missing capability
- Multi-table query where one table is unauthorized

All must fail before implementation and pass after.

---

#### Green-Flag Tests (Required)

- Authorized role can query permitted table
- Authorized role with correct capability can use AS OF
- Multi-table query where all tables are permitted

---

### 5. Add Red-Flag Tests for Unauthorized Access

#### Objective
Authorization failures must be **first-class Red-Flags**, not runtime surprises.

#### Requirements

- Authorization Red-Flags must be engine-independent
- Tests must fail without any engine running
- Tests must not rely on adapter behavior

#### Forbidden Tests

- “engine returns permission denied”
- “database-level ACL failure”

Authorization is enforced **before** engines.

---

### 6. Decide and Enforce Table Naming Rules

#### Objective
Eliminate ambiguity in table identity.

#### Decision (MANDATED)

**Schema-qualified table names are required.**

Format:
```
<schema>.<table>
```

Examples:
- analytics.sales_orders ✅
- sales_orders ❌

---

#### Rationale

- Prevents name collisions
- Simplifies authorization mapping
- Aligns with enterprise SQL expectations
- Avoids engine-specific defaults

---

#### Enforcement Rules

- Queries referencing unqualified table names MUST fail
- Virtual table registration MUST reject unqualified names
- Error messages must explain required format

Example:
```
Invalid table reference: 'sales_orders'
Fully-qualified name required: <schema>.<table>
```

---

#### Red-Flag Tests (Required)

- Query using unqualified table name
- Virtual table registered without schema
- Mixed qualified and unqualified table references

---

#### Green-Flag Tests (Required)

- Fully-qualified table names resolve correctly
- Authorization works with qualified names
- Multi-schema queries resolve deterministically

---

## Out-of-Scope (Explicit)

Phase 2 does NOT include:
- Column-level security
- Row-level security
- Role inheritance
- External IAM integration
- Dynamic policy evaluation

Any PR adding the above will be rejected.

---

## Acceptance Criteria

Phase 2 is complete only if:

1. All access is deny-by-default
2. Unauthorized queries fail before planning
3. Authorization errors are explicit and human-readable
4. Schema-qualified names are mandatory and enforced
5. All Phase 2 Red-Flag tests fail before implementation and pass after

---

## Failure Policy

If authorization cannot be proven:
**The query MUST fail.**

There is no fallback and no warning-only mode.

---

## Final Statement

Phase 2 does not make the system complex.
It makes the system **safe to share**.

Multi-user safety is not optional.
