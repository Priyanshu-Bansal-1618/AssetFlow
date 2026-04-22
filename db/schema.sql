CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ENUM TYPES
CREATE TYPE ledger_event_type AS ENUM (
    'DEPOSIT',
    'WITHDRAW',
    'ALLOCATE',
    'DEALLOCATE',
    'YIELD_CREDIT',
    'DEAL_CLOSE',
    'YIELD_EARNED',
    'YIELD_DISTRIBUTED'
);

CREATE TYPE allocation_type AS ENUM (
    'LEASING',
    'COLLATERAL_LOCK'
);

CREATE TYPE allocation_status AS ENUM (
    'ACTIVE',
    'DEALLOCATED'
);

CREATE TYPE deal_status AS ENUM (
    'ACTIVE',
    'MATURED',
    'DEFAULTED',
    'CANCELLED'
);

CREATE TYPE counterparty_entity_type AS ENUM (
    'JEWELLER',
    'REFINER',
    'TRADING_DESK'
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
CREATE TABLE accounts (
    id                      SERIAL          PRIMARY KEY,
    user_id                 INTEGER         NOT NULL UNIQUE REFERENCES users(id) ON DELETE RESTRICT,
    balance_grams           NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_deposited_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_withdrawn_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    allocated_grams         NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_balance_non_negative         CHECK (balance_grams >= 0),
    CONSTRAINT chk_allocated_non_negative       CHECK (allocated_grams >= 0),
    CONSTRAINT chk_total_deposited_non_negative CHECK (total_deposited_grams >= 0),
    CONSTRAINT chk_total_withdrawn_non_negative CHECK (total_withdrawn_grams >= 0),
    CONSTRAINT chk_balance_sanity CHECK (
        balance_grams + allocated_grams <= total_deposited_grams - total_withdrawn_grams + 0.000001
    )
);

-- TABLE: sessions
CREATE TABLE sessions (
    session_token   TEXT            PRIMARY KEY,
    user_id         INTEGER         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW() + INTERVAL '24 hours',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);

-- TABLE: counterparties  (v2)
CREATE TABLE counterparties (
    id                  SERIAL                      PRIMARY KEY,
    name                VARCHAR(255)                NOT NULL UNIQUE,
    entity_type         counterparty_entity_type    NOT NULL,
    credit_rating       VARCHAR(8),
    max_exposure_grams  NUMERIC(18, 6)              NOT NULL DEFAULT 0,
    is_active           BOOLEAN                     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_max_exposure_nn CHECK (max_exposure_grams >= 0)
);

-- TABLE: allocations
-- is_pooled: TRUE = vault-level (v2 deal/collateral). FALSE = legacy user allocation.
CREATE TABLE allocations (
    id                  SERIAL              PRIMARY KEY,
    account_id          INTEGER             NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    allocation_type     allocation_type     NOT NULL,
    status              allocation_status   NOT NULL DEFAULT 'ACTIVE',
    amount_grams        NUMERIC(18, 6)      NOT NULL,
    yield_rate_bps      INTEGER             NOT NULL DEFAULT 0,
    is_pooled           BOOLEAN             NOT NULL DEFAULT FALSE,
    maturity_date       DATE,
    allocated_at        TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    deallocated_at      TIMESTAMPTZ,
    closed_at           TIMESTAMPTZ,
    notes               TEXT,

    CONSTRAINT chk_alloc_amount_positive  CHECK (amount_grams > 0),
    CONSTRAINT chk_yield_rate_non_neg     CHECK (yield_rate_bps >= 0),
    CONSTRAINT chk_dealloc_after_alloc    CHECK (
        deallocated_at IS NULL OR deallocated_at >= allocated_at
    )
);

-- TABLE: allocation_deals  (v2)
-- One deal per LEASING allocation. COLLATERAL allocations have no deal.
CREATE TABLE allocation_deals (
    id                  SERIAL          PRIMARY KEY,
    allocation_id       INTEGER         NOT NULL UNIQUE REFERENCES allocations(id) ON DELETE RESTRICT,
    counterparty_id     INTEGER         NOT NULL REFERENCES counterparties(id) ON DELETE RESTRICT,
    amount_grams        NUMERIC(18, 6)  NOT NULL,
    yield_rate_bps      INTEGER         NOT NULL DEFAULT 0,
    start_date          DATE            NOT NULL DEFAULT CURRENT_DATE,
    maturity_date       DATE,
    status              deal_status     NOT NULL DEFAULT 'ACTIVE',
    closed_at           TIMESTAMPTZ,
    deal_reference      VARCHAR(128)    UNIQUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_deal_amount_positive      CHECK (amount_grams > 0),
    CONSTRAINT chk_deal_yield_rate_nn        CHECK (yield_rate_bps >= 0),
    CONSTRAINT chk_deal_maturity_after_start CHECK (
        maturity_date IS NULL OR maturity_date >= start_date
    ),
    CONSTRAINT chk_deal_closed_requires_status CHECK (
        closed_at IS NULL OR status IN ('MATURED', 'DEFAULTED', 'CANCELLED')
    )
);

-- TABLE: ledger_events
-- Append-only. deal_id links DEAL_CLOSE/YIELD_EARNED events to their deal.
CREATE TABLE ledger_events (
    id              BIGSERIAL           PRIMARY KEY,
    account_id      INTEGER             NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    event_type      ledger_event_type   NOT NULL,
    amount_grams    NUMERIC(18, 6)      NOT NULL,
    balance_after   NUMERIC(18, 6)      NOT NULL,
    reference_id    INTEGER,
    deal_id         INTEGER             REFERENCES allocation_deals(id) DEFERRABLE INITIALLY DEFERRED,
    notes           TEXT,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_amount_positive  CHECK (amount_grams > 0),
    CONSTRAINT chk_balance_after_nn CHECK (balance_after >= 0)
);

-- TABLE: system_state  (single row)
CREATE TABLE system_state (
    id                              INTEGER         PRIMARY KEY DEFAULT 1,
    total_deposited_grams           NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_withdrawn_grams           NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    vault_gold_grams                NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    allocated_gold_grams            NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    pending_withdrawal_grams        NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_yield_earned_grams        NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    total_yield_distributed_grams   NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    system_profit_grams             NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    last_updated_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT single_row       CHECK (id = 1),
    CONSTRAINT chk_vault_nn     CHECK (vault_gold_grams >= 0),
    CONSTRAINT chk_allocated_nn CHECK (allocated_gold_grams >= 0),
    CONSTRAINT chk_pending_nn   CHECK (pending_withdrawal_grams >= 0),
    CONSTRAINT chk_system_invariant CHECK (
        ABS(
            (total_deposited_grams - total_withdrawn_grams)
            - (vault_gold_grams + allocated_gold_grams + pending_withdrawal_grams)
        ) < 0.001
    )
);

INSERT INTO system_state (id) VALUES (1);

-- TABLE: yield_events  (v2)
-- One row per deal/collateral close. allocation_id for collateral traceability.
CREATE TABLE yield_events (
    id                          SERIAL          PRIMARY KEY,
    deal_id                     INTEGER         REFERENCES allocation_deals(id) ON DELETE RESTRICT,
    allocation_id               INTEGER         REFERENCES allocations(id) ON DELETE RESTRICT,
    gross_yield_grams           NUMERIC(18, 6)  NOT NULL,
    system_fee_grams            NUMERIC(18, 6)  NOT NULL,
    distributable_yield_grams   NUMERIC(18, 6)  NOT NULL,
    system_fee_bps              INTEGER         NOT NULL,
    total_user_balance_snapshot NUMERIC(18, 6)  NOT NULL,
    distribution_completed      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    distributed_at              TIMESTAMPTZ,

    CONSTRAINT chk_yield_gross_positive    CHECK (gross_yield_grams >= 0),
    CONSTRAINT chk_yield_fee_nn            CHECK (system_fee_grams >= 0),
    CONSTRAINT chk_yield_distributable_nn  CHECK (distributable_yield_grams >= 0),
    CONSTRAINT chk_yield_fee_bps_range     CHECK (system_fee_bps BETWEEN 0 AND 10000),
    CONSTRAINT chk_yield_has_source        CHECK (deal_id IS NOT NULL OR allocation_id IS NOT NULL),
    CONSTRAINT chk_yield_dist_correct      CHECK (
        ABS(distributable_yield_grams - (gross_yield_grams - system_fee_grams)) < 0.000001
    )
);

-- TABLE: yield_distributions  (v2)
-- Per-user yield credit audit trail for each yield_event.
CREATE TABLE yield_distributions (
    id                      SERIAL          PRIMARY KEY,
    yield_event_id          INTEGER         NOT NULL REFERENCES yield_events(id) ON DELETE RESTRICT,
    account_id              INTEGER         NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    user_balance_snapshot   NUMERIC(18, 6)  NOT NULL,
    share_fraction          NUMERIC(18, 10) NOT NULL,
    yield_grams             NUMERIC(18, 6)  NOT NULL,
    ledger_event_id         BIGINT          REFERENCES ledger_events(id),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_dist_balance_positive CHECK (user_balance_snapshot > 0),
    CONSTRAINT chk_dist_share_range      CHECK (share_fraction > 0 AND share_fraction <= 1),
    CONSTRAINT chk_dist_yield_positive   CHECK (yield_grams > 0),
    UNIQUE (yield_event_id, account_id)
);

-- TABLE: system_config  (v2)
CREATE TABLE system_config (
    key         VARCHAR(64)     PRIMARY KEY,
    value       TEXT            NOT NULL,
    description TEXT,
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

INSERT INTO system_config (key, value, description) VALUES
    ('system_fee_bps',              '2500', 'Platform cut of yield in basis points (2500 = 25%)'),
    ('min_deal_amount_grams',       '1.0',  'Minimum grams for a lease deal'),
    ('max_counterparty_exposure_pct','40',  'Max % of vault any single counterparty can hold')
ON CONFLICT (key) DO NOTHING;

-- INDEXES
CREATE INDEX idx_accounts_user_id          ON accounts(user_id);
CREATE INDEX idx_sessions_user_id          ON sessions(user_id);
CREATE INDEX idx_sessions_expires_active   ON sessions(expires_at, is_active);
CREATE INDEX idx_ledger_account_id         ON ledger_events(account_id);
CREATE INDEX idx_ledger_created_at         ON ledger_events(created_at DESC);
CREATE INDEX idx_ledger_account_created    ON ledger_events(account_id, created_at DESC);
CREATE INDEX idx_ledger_event_type         ON ledger_events(event_type);
CREATE INDEX idx_ledger_reference_id       ON ledger_events(reference_id) WHERE reference_id IS NOT NULL;
CREATE INDEX idx_ledger_deal_id            ON ledger_events(deal_id) WHERE deal_id IS NOT NULL;
CREATE INDEX idx_alloc_account_id          ON allocations(account_id);
CREATE INDEX idx_alloc_status              ON allocations(status);
CREATE INDEX idx_alloc_account_status      ON allocations(account_id, status);
CREATE INDEX idx_alloc_type                ON allocations(allocation_type);
CREATE INDEX idx_alloc_is_pooled           ON allocations(is_pooled);
CREATE INDEX idx_cpty_active               ON counterparties(is_active);
CREATE INDEX idx_cpty_entity_type          ON counterparties(entity_type);
CREATE INDEX idx_deal_allocation_id        ON allocation_deals(allocation_id);
CREATE INDEX idx_deal_counterparty_id      ON allocation_deals(counterparty_id);
CREATE INDEX idx_deal_status               ON allocation_deals(status);
CREATE INDEX idx_deal_cpty_status          ON allocation_deals(counterparty_id, status);
CREATE INDEX idx_deal_maturity_date        ON allocation_deals(maturity_date) WHERE maturity_date IS NOT NULL;
CREATE INDEX idx_yield_event_deal_id       ON yield_events(deal_id) WHERE deal_id IS NOT NULL;
CREATE INDEX idx_yield_event_alloc_id      ON yield_events(allocation_id) WHERE allocation_id IS NOT NULL;
CREATE INDEX idx_yield_event_dist          ON yield_events(distribution_completed);
CREATE INDEX idx_yield_dist_account        ON yield_distributions(account_id);
CREATE INDEX idx_yield_dist_event          ON yield_distributions(yield_event_id);