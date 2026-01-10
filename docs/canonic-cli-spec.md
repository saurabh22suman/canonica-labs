# canonic CLI – Command Specification

## Philosophy

The **canonic CLI** is a **control interface**, not a convenience shell.

It exists to:
- configure the system
- validate safety
- explain behavior
- integrate with CI/CD

It does **not**:
- replace SQL clients
- execute ad-hoc workflows
- hide failures

If the CLI cannot explain *why* something fails, it is incomplete.

---

## Binary Name

```
canonic
```

---

## Global Flags

Applicable to all commands:

```
--config <path>        Path to config file (default: ~/.canonic/config.yaml)
--endpoint <url>       Control plane endpoint
--token <token>        Auth token (overrides config)
--json                 Machine-readable output
--quiet                Suppress non-essential output
--debug                Verbose debug logs
```

---

## Command Groups

```
canonic
├── auth
├── table
├── query
├── engine
├── policy
├── audit
├── doctor
└── version
```

Each group owns a single responsibility.
No command crosses boundaries.

---

## auth – Authentication

### canonic auth login

Authenticate with the control plane.

```
canonic auth login
```

- Stores token locally
- No permission validation

---

### canonic auth status

Display authentication status.

```
canonic auth status
```

Displays:
- identity
- roles
- token expiry

---

## table – Virtual Table Management (CORE)

### canonic table register

Register or update a virtual table.

```
canonic table register <file.yaml>
```

Behavior:
- Validates schema
- Validates capabilities
- Fails on ambiguity

---

### canonic table validate

Validate a table definition without registering.

```
canonic table validate <file.yaml>
```

Used in CI/CD pipelines.

---

### canonic table describe

Describe a registered virtual table.

```
canonic table describe <table_name>
```

Displays:
- capabilities
- constraints
- physical sources
- engine compatibility
- health status

---

### canonic table list

List registered virtual tables.

```
canonic table list
```

Optional filters:
```
--engine
--capability
--constraint
```

---

### canonic table delete

Delete a virtual table.

```
canonic table delete <table_name>
```

Requires confirmation unless `--force` is provided.

---

## query – Query Interface

### canonic query

Execute a SQL query through the gateway.

```
canonic query "<SQL>"
```

Default behavior:
- Validates semantics
- Routes to engine
- Streams results

---

### canonic query explain

Explain how a query will be executed.

```
canonic query explain "<SQL>"
```

Shows:
- tables referenced
- capabilities required
- selected engine
- blocked operations (if any)

---

### canonic query validate

Validate a query without execution.

```
canonic query validate "<SQL>"
```

Fails if:
- unsupported operations
- missing capabilities
- ambiguous routing

---

## engine – Engine Inspection

### canonic engine list

List available engines.

```
canonic engine list
```

Shows:
- engine type
- status
- supported capabilities

---

### canonic engine describe

Describe a specific engine.

```
canonic engine describe <engine_name>
```

---

## policy – Capability & Constraint Policies

### canonic policy list

List active policies.

```
canonic policy list
```

---

### canonic policy validate

Validate policy definitions.

```
canonic policy validate <file.yaml>
```

---

## audit – Query Audit Logs

### canonic audit query

Search query audit logs.

```
canonic audit query --table analytics.sales_orders
```

Filters:
```
--user
--engine
--status
--since
```

---

## doctor – Diagnostics

### canonic doctor

Run system diagnostics.

```
canonic doctor
```

Checks:
- connectivity
- auth
- engine health
- metadata integrity

---

## version

### canonic version

Display CLI and server version.

```
canonic version
```

---

## Exit Codes

```
0   Success
1   Validation error (Red-Flag)
2   Auth error
3   Engine error
4   Internal error
```

Failures must be explicit and actionable.
