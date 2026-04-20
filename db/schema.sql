-- Schema: Tables, Constraints, Indexes


-- Enable UUID support
CREATE EXTENSION IF NOT EXISTS "pgcrypto";


-- ENUM TYPES
CREATE TYPE ledger_event_type AS ENUM (
    'DEPOSIT',
    'WITHDRAW',
    'ALLOCATE',
    'DEALLOCATE',
    'YIELD_CREDIT'
);

CREATE TYPE allocation_type AS ENUM (
    'LEASING',
    'COLLATERAL_LOCK'
);

CREATE TYPE allocation_status AS ENUM (
    'ACTIVE',
    'DEALLOCATED'
);


-- TABLE: users
CREATE TABLE users (
    id              SERIAL          PRIMARY KEY,
    username        VARCHAR(64)     NOT NULL UNIQUE,
    email           VARCHAR(255)    NOT NULL UNIQUE,
    password_hash   TEXT            NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,

    CONSTRAINT chk_username_length CHECK (char_length(username) >= 3),
    CONSTRAINT chk_email_format    CHECK (email LIKE '%@%')
);


-- TABLE: accounts
-- Each user has exactly one gold account.
-- balance_grams is the AVAILABLE (unallocated, non-pending) balance.
-- This is a DERIVED value maintained by triggers — do not write directly.
CREATE TABLE accounts (
    id                      SERIAL          PRIMARY KEY,
    user_id                 INTEGER         NOT NULL UNIQUE REFERENCES users(id) ON DELETE RESTRICT,
    balance_grams           NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_deposited_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_withdrawn_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    allocated_grams         NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_balance_non_negative        CHECK (balance_grams >= 0),
    CONSTRAINT chk_allocated_non_negative      CHECK (allocated_grams >= 0),
    CONSTRAINT chk_total_deposited_non_negative CHECK (total_deposited_grams >= 0),
    CONSTRAINT chk_total_withdrawn_non_negative CHECK (total_withdrawn_grams >= 0),
    -- Available balance cannot exceed total deposited minus withdrawn
    CONSTRAINT chk_balance_sanity CHECK (
        balance_grams + allocated_grams <= total_deposited_grams - total_withdrawn_grams + 0.000001
    )
);


-- TABLE: ledger_events
-- Append-only immutable event log. Every mutation flows through here first.
CREATE TABLE ledger_events (
    id              BIGSERIAL       PRIMARY KEY,
    account_id      INTEGER         NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    event_type      ledger_event_type NOT NULL,
    amount_grams    NUMERIC(18, 6)  NOT NULL,
    balance_after   NUMERIC(18, 6)  NOT NULL,    -- snapshot of balance post-event
    reference_id    INTEGER,                     -- links to allocations.id when relevant
    notes           TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_amount_positive     CHECK (amount_grams > 0),
    CONSTRAINT chk_balance_after_nn    CHECK (balance_after >= 0)
);
-- ledger_events is append-only. Enforced via trigger (see triggers.sql).


-- TABLE: allocations
-- Tracks gold allocated to leasing or collateral lock.
CREATE TABLE allocations (
    id                  SERIAL              PRIMARY KEY,
    account_id          INTEGER             NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    allocation_type     allocation_type     NOT NULL,
    status              allocation_status   NOT NULL DEFAULT 'ACTIVE',
    amount_grams        NUMERIC(18, 6)      NOT NULL,
    yield_rate_bps      INTEGER             NOT NULL DEFAULT 0,  -- basis points (1 bps = 0.01%)
    allocated_at        TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    deallocated_at      TIMESTAMPTZ,
    notes               TEXT,

    CONSTRAINT chk_alloc_amount_positive  CHECK (amount_grams > 0),
    CONSTRAINT chk_yield_rate_non_neg     CHECK (yield_rate_bps >= 0),
    CONSTRAINT chk_dealloc_after_alloc    CHECK (
        deallocated_at IS NULL OR deallocated_at >= allocated_at
    )
);


-- TABLE: system_state
-- Single-row global invariant tracker.
-- Maintained by triggers on ledger_events.
CREATE TABLE system_state (
    id                      INTEGER         PRIMARY KEY DEFAULT 1,
    total_deposited_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_withdrawn_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    vault_gold_grams        NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    allocated_gold_grams    NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    pending_withdrawal_grams NUMERIC(18, 6) NOT NULL DEFAULT 0,
    last_updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT single_row            CHECK (id = 1),
    CONSTRAINT chk_vault_nn          CHECK (vault_gold_grams >= 0),
    CONSTRAINT chk_allocated_nn      CHECK (allocated_gold_grams >= 0),
    CONSTRAINT chk_pending_nn        CHECK (pending_withdrawal_grams >= 0),
    -- Core invariant: TOTAL_DEPOSITED - TOTAL_WITHDRAWN = VAULT + ALLOCATED + PENDING
    CONSTRAINT chk_system_invariant CHECK (
        ABS(
            (total_deposited_grams - total_withdrawn_grams)
            - (vault_gold_grams + allocated_gold_grams + pending_withdrawal_grams)
        ) < 0.001  -- tolerance for floating point
    )
);

-- Seed the single system_state row
INSERT INTO system_state (id) VALUES (1);


-- TABLE: sessions
-- Simple server-side session store.
CREATE TABLE sessions (
    session_token   TEXT            PRIMARY KEY,
    user_id         INTEGER         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW() + INTERVAL '24 hours',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);


-- INDEXES

-- accounts: look up by user_id constantly (auth flow, every operation)
CREATE INDEX idx_accounts_user_id ON accounts(user_id);

-- ledger_events: primary query patterns are by account + time
CREATE INDEX idx_ledger_account_id       ON ledger_events(account_id);
CREATE INDEX idx_ledger_created_at       ON ledger_events(created_at DESC);
CREATE INDEX idx_ledger_account_created  ON ledger_events(account_id, created_at DESC);
CREATE INDEX idx_ledger_event_type       ON ledger_events(event_type);

-- ledger reference_id: joins to allocations
CREATE INDEX idx_ledger_reference_id     ON ledger_events(reference_id)
    WHERE reference_id IS NOT NULL;

-- allocations: look up active allocations per account
CREATE INDEX idx_alloc_account_id        ON allocations(account_id);
CREATE INDEX idx_alloc_status            ON allocations(status);
CREATE INDEX idx_alloc_account_status    ON allocations(account_id, status);
CREATE INDEX idx_alloc_type              ON allocations(allocation_type);

-- sessions: look up token (PK covers this), also need user lookup for invalidation
CREATE INDEX idx_sessions_user_id        ON sessions(user_id);
CREATE INDEX idx_sessions_expires_active ON sessions(expires_at, is_active);

-- users: username and email are already UNIQUE (implicitly indexed)
-- i.e. PostgreSQL automatically creates indexes for them