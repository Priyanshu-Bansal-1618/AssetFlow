# Gold Monetization Infrastructure Simulator

A backend-focused project demonstrating strong database design, transactional safety, and ledger-based accounting using Python (FastAPI) and PostgreSQL.

---

## Architecture Overview

```
Client (Browser)
      │
      ▼
FastAPI (Routes → Services)       ← thin Python layer, input validation only
      │
      ▼
PostgreSQL
  ├── Stored Procedures           ← all mutation logic lives here
  ├── Triggers                    ← invariant enforcement, ledger immutability
  └── Ledger (append-only)        ← single source of truth
```

**Core design philosophy:**
- The **ledger is the source of truth**. Every gold movement generates an immutable ledger event.
- **Stored procedures own all transactional logic**. Python never mutates balances directly.
- **Triggers enforce invariants** at the database layer, not the application layer.
- Account balances are **derived state** maintained by triggers reading ledger events.

---

## Project Structure

```
gold_simulator/
├── app/
│   ├── main.py                   # FastAPI app, lifespan, router registration
│   ├── dependencies.py           # Session auth dependency, account resolution
│   ├── routes/
│   │   ├── auth.py               # /auth/register, /auth/login, /auth/logout
│   │   ├── accounts.py           # /accounts/deposit, /accounts/withdraw
│   │   ├── allocations.py        # /allocations/allocate, /allocations/deallocate
│   │   └── ledger.py             # /ledger/ (read-only audit trail)
│   ├── services/
│   │   ├── auth_service.py       # bcrypt hashing, session creation
│   │   ├── account_service.py    # deposit/withdraw — calls DB procedures
│   │   └── allocation_service.py # allocate/deallocate/yield — calls DB procedures
│   └── db/
│       └── connection.py         # psycopg2 ThreadedConnectionPool
├── db/
│   ├── schema.sql                # Tables, constraints, indexes
│   ├── triggers.sql              # All triggers + helper functions
│   └── procedures.sql            # Stored procedures / functions
├── frontend/
│   └── templates/
│       ├── base.html
│       ├── login.html
│       ├── register.html
│       ├── dashboard.html
│       ├── allocations.html
│       └── ledger.html
├── requirements.txt
└── .env.example
```

---

## Database Schema

### Tables

| Table | Purpose |
|---|---|
| `users` | Credentials, auth |
| `accounts` | Per-user gold balance (derived, trigger-maintained) |
| `ledger_events` | Immutable append-only event log (source of truth) |
| `allocations` | Active/historical gold allocation positions |
| `system_state` | Single-row global invariant tracker |
| `sessions` | Server-side session store |

### System Invariant

The following must always hold:

```
TOTAL_DEPOSITED - TOTAL_WITHDRAWN = VAULT_GOLD + ALLOCATED_GOLD + PENDING_WITHDRAWALS
```

This is enforced by:
1. A `CHECK` constraint on the `system_state` table
2. `fn_assert_system_invariant()` called after every ledger insert
3. `trg_system_state_guard` blocking any vault/allocated from going negative

---

## Triggers

| Trigger | On | Purpose |
|---|---|---|
| `trg_ledger_no_update` | `UPDATE` on `ledger_events` | Block — ledger is immutable |
| `trg_ledger_no_delete` | `DELETE` on `ledger_events` | Block — ledger is immutable |
| `trg_account_balance_guard` | `BEFORE UPDATE` on `accounts` | Prevent negative balances, enforce sanity |
| `trg_sync_system_state` | `AFTER INSERT` on `ledger_events` | Auto-update system_state totals |
| `trg_allocation_status_guard` | `BEFORE UPDATE` on `allocations` | Prevent re-activation, immutable amounts |
| `trg_account_no_delete_with_balance` | `BEFORE DELETE` on `accounts` | Block deletion with non-zero balance |
| `trg_system_state_guard` | `BEFORE UPDATE` on `system_state` | Vault/allocated cannot go negative |

---

## Stored Procedures

All mutations go through these — never raw `UPDATE` from Python:

| Procedure | Description |
|---|---|
| `deposit_gold(account_id, amount, notes)` | Lock account → update balance → insert ledger event |
| `withdraw_gold(account_id, amount, notes)` | Lock account → verify available balance → update → ledger |
| `allocate_gold(account_id, amount, type, bps, notes)` | Lock account → create allocation → move to allocated → ledger |
| `deallocate_gold(account_id, allocation_id, notes)` | Lock allocation + account → mark DEALLOCATED → return gold → ledger |
| `credit_yield(allocation_id, notes)` | Calculate yield (amount × bps / 10000) → credit to balance → ledger |
| `get_account_balance_from_ledger(account_id)` | Reconstruct balance purely from ledger history |
| `verify_ledger_consistency(account_id)` | Compare stored balance vs ledger-reconstructed balance |

---

## Indexing Strategy

```sql
-- accounts: every auth request resolves user_id → account
CREATE INDEX idx_accounts_user_id ON accounts(user_id);

-- ledger: primary queries are by account + time descending
CREATE INDEX idx_ledger_account_created ON ledger_events(account_id, created_at DESC);
CREATE INDEX idx_ledger_event_type      ON ledger_events(event_type);
CREATE INDEX idx_ledger_reference_id    ON ledger_events(reference_id) WHERE reference_id IS NOT NULL;

-- allocations: frequent filter is account + status
CREATE INDEX idx_alloc_account_status ON allocations(account_id, status);
```

**Why these indexes:**
- `idx_ledger_account_created`: The most common ledger query is "give me the last N events for account X". Composite index on `(account_id, created_at DESC)` satisfies this with an index-only scan.
- `idx_alloc_account_status`: Allocation pages always filter `WHERE account_id = ? AND status = 'ACTIVE'`.
- `idx_ledger_reference_id` with partial index (`WHERE reference_id IS NOT NULL`): Most ledger events have no reference; partial index is smaller and faster.

---

## Concurrency Handling

### Strategy: Pessimistic Locking via `SELECT FOR UPDATE`

Every stored procedure that modifies an account begins with:
```sql
SELECT * FROM accounts WHERE id = $1 FOR UPDATE;
```

This acquires a row-level exclusive lock on the account row for the duration of the transaction. Any concurrent transaction attempting to lock the same row will block until the first commits or rolls back.

### Deadlock Prevention

For `deallocate_gold`, two rows are locked (allocation + account). To prevent deadlocks from inconsistent lock ordering, the allocation row is always locked **before** the account row. This consistent ordering means two concurrent deallocations cannot deadlock each other.

### Advisory Lock for System State

`fn_sync_system_state` (the trigger that updates the single `system_state` row) uses:
```sql
PERFORM pg_advisory_xact_lock(1);
```
This is a transaction-scoped advisory lock that serializes all system_state updates, preventing lost-update races on the global totals without a separate mutex.

### Connection Pool

The app uses `psycopg2.ThreadedConnectionPool(minconn=2, maxconn=20)`. Each request borrows a connection, runs its transaction, and returns it. FastAPI's synchronous route handlers share the pool safely because psycopg2 connections are not thread-safe but the pool handles allocation per-thread.

---

## API Endpoints

### Authentication
```
GET  /auth/login          Login page
POST /auth/login          Submit credentials → sets session cookie
GET  /auth/register       Registration page
POST /auth/register       Create user + account
POST /auth/logout         Invalidate session
```

### Accounts
```
GET  /accounts/dashboard  Dashboard (HTML)
POST /accounts/deposit    Deposit gold (form)
POST /accounts/withdraw   Withdraw gold (form)

GET  /accounts/api/balance       JSON balance
POST /accounts/api/deposit       JSON deposit
POST /accounts/api/withdraw      JSON withdraw
GET  /accounts/api/system-state  JSON system state
```

### Allocations
```
GET  /allocations/                   Allocations page
POST /allocations/allocate           Allocate gold
POST /allocations/deallocate/{id}    Deallocate position
POST /allocations/yield/{id}         Credit yield

GET  /allocations/api/list           JSON list allocations
POST /allocations/api/allocate       JSON allocate
POST /allocations/api/deallocate/{id} JSON deallocate
```

### Ledger
```
GET /ledger/               Paginated ledger view (HTML)
GET /ledger/verify         Audit: compare stored vs reconstructed balance
GET /ledger/api/events     JSON paginated ledger events
```

---

## Setup & Running

### 1. Create the database

```bash
createdb assetflow
psql assetflow < db/schema.sql
psql assetflow < db/triggers.sql
psql assetflow < db/procedures.sql
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit DATABASE_URL in .env
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the server

```bash
uvicorn app.main:app --reload --port 8000
```

Visit: http://localhost:8000

---

## Key Design Decisions

### Why stored procedures instead of ORM?

The core invariant (`TOTAL = VAULT + ALLOCATED + PENDING`) must hold atomically. Stored procedures run entirely inside a single database transaction, making partial failure impossible. An ORM running equivalent Python logic across multiple round-trips would require careful manual transaction management and could silently violate the invariant on application crashes.

### Why append-only ledger?

- **Auditability**: Every state change is permanently recorded with a timestamp.
- **Recoverability**: `verify_ledger_consistency()` reconstructs the current balance from scratch by replaying events — if it matches `accounts.balance_grams`, the system is internally consistent.
- **Debugging**: A balance discrepancy can be traced to the exact event that caused it.

### Why triggers for system_state?

`system_state` could be computed on demand with a `SUM()` query over all ledger events, but that scales poorly. Triggers maintain a running total that is O(1) to read. The trade-off is trigger complexity, but triggers are simpler than background jobs or application-level cache invalidation.

### Numeric precision

All gold amounts use `NUMERIC(18, 6)` — exact decimal arithmetic with 6 decimal places (microgram precision). `FLOAT` would introduce rounding errors that accumulate over time and would eventually violate the invariant check tolerance.
