
-- Helpers

CREATE OR REPLACE FUNCTION get_system_fee_bps()
RETURNS INTEGER AS $$
DECLARE v_bps INTEGER;
BEGIN
    SELECT value::INTEGER INTO v_bps FROM system_config WHERE key = 'system_fee_bps';
    RETURN COALESCE(v_bps, 2500);
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION get_system_account_id()
RETURNS INTEGER AS $$
DECLARE v_id INTEGER;
BEGIN
    SELECT a.id INTO v_id FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username = '__system__';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'System account not found. Run: SELECT seed_system_account();';
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION seed_system_account()
RETURNS INTEGER AS $$
DECLARE v_user_id INTEGER; v_account_id INTEGER;
BEGIN
    INSERT INTO users (username, email, password_hash, is_active)
    VALUES ('__system__', 'system@internal', '__NOT_A_REAL_HASH__', TRUE)
    ON CONFLICT (username) DO NOTHING;
    SELECT id INTO v_user_id FROM users WHERE username = '__system__';
    INSERT INTO accounts (user_id) VALUES (v_user_id) ON CONFLICT (user_id) DO NOTHING;
    SELECT id INTO v_account_id FROM accounts WHERE user_id = v_user_id;
    RETURN v_account_id;
END;
$$ LANGUAGE plpgsql;

-- deposit_gold
CREATE OR REPLACE FUNCTION deposit_gold(
    p_account_id   INTEGER,
    p_amount_grams NUMERIC(18, 6),
    p_notes        TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account     accounts%ROWTYPE;
    v_new_balance NUMERIC(18, 6);
    v_ledger_id   BIGINT;
BEGIN
    IF p_amount_grams <= 0 THEN
        RAISE EXCEPTION 'Deposit amount must be positive, got: %', p_amount_grams;
    END IF;
    SELECT * INTO v_account FROM accounts WHERE id = p_account_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Account not found: id=%', p_account_id; END IF;
    v_new_balance := v_account.balance_grams + p_amount_grams;
    UPDATE accounts SET
        balance_grams         = v_new_balance,
        total_deposited_grams = total_deposited_grams + p_amount_grams
    WHERE id = p_account_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
    VALUES (p_account_id, 'DEPOSIT', p_amount_grams, v_new_balance, p_notes)
    RETURNING id INTO v_ledger_id;
    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- withdraw_gold
CREATE OR REPLACE FUNCTION withdraw_gold(
    p_account_id   INTEGER,
    p_amount_grams NUMERIC(18, 6),
    p_notes        TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account     accounts%ROWTYPE;
    v_new_balance NUMERIC(18, 6);
    v_ledger_id   BIGINT;
BEGIN
    IF p_amount_grams <= 0 THEN
        RAISE EXCEPTION 'Withdrawal amount must be positive, got: %', p_amount_grams;
    END IF;
    SELECT * INTO v_account FROM accounts WHERE id = p_account_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Account not found: id=%', p_account_id; END IF;
    IF v_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION 'Insufficient available balance: requested=% available=% (%g allocated and unavailable)',
            p_amount_grams, v_account.balance_grams, v_account.allocated_grams;
    END IF;
    v_new_balance := v_account.balance_grams - p_amount_grams;
    UPDATE accounts SET
        balance_grams         = v_new_balance,
        total_withdrawn_grams = total_withdrawn_grams + p_amount_grams
    WHERE id = p_account_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
    VALUES (p_account_id, 'WITHDRAW', p_amount_grams, v_new_balance, p_notes)
    RETURNING id INTO v_ledger_id;
    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- allocate_gold  (legacy user-level allocation — kept for backward compat)
CREATE OR REPLACE FUNCTION allocate_gold(
    p_account_id      INTEGER,
    p_amount_grams    NUMERIC(18, 6),
    p_allocation_type allocation_type,
    p_yield_rate_bps  INTEGER DEFAULT 0,
    p_notes           TEXT DEFAULT NULL
)
RETURNS TABLE(ledger_event_id BIGINT, allocation_id INTEGER) AS $$
DECLARE
    v_account     accounts%ROWTYPE;
    v_alloc_id    INTEGER;
    v_ledger_id   BIGINT;
    v_new_balance NUMERIC(18, 6);
BEGIN
    IF p_amount_grams <= 0 THEN RAISE EXCEPTION 'Allocation amount must be positive, got: %', p_amount_grams; END IF;
    IF p_yield_rate_bps < 0 THEN RAISE EXCEPTION 'Yield rate cannot be negative, got: %', p_yield_rate_bps; END IF;
    SELECT * INTO v_account FROM accounts WHERE id = p_account_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Account not found: id=%', p_account_id; END IF;
    IF v_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION 'Insufficient available balance for allocation: requested=% available=%',
            p_amount_grams, v_account.balance_grams;
    END IF;
    INSERT INTO allocations (account_id, allocation_type, amount_grams, yield_rate_bps, is_pooled, notes)
    VALUES (p_account_id, p_allocation_type, p_amount_grams, p_yield_rate_bps, FALSE, p_notes)
    RETURNING id INTO v_alloc_id;
    v_new_balance := v_account.balance_grams - p_amount_grams;
    UPDATE accounts SET
        balance_grams   = v_new_balance,
        allocated_grams = allocated_grams + p_amount_grams
    WHERE id = p_account_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (p_account_id, 'ALLOCATE', p_amount_grams, v_new_balance, v_alloc_id, p_notes)
    RETURNING id INTO v_ledger_id;
    RETURN QUERY SELECT v_ledger_id, v_alloc_id;
END;
$$ LANGUAGE plpgsql;

-- deallocate_gold  (legacy)
CREATE OR REPLACE FUNCTION deallocate_gold(
    p_account_id    INTEGER,
    p_allocation_id INTEGER,
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account     accounts%ROWTYPE;
    v_allocation  allocations%ROWTYPE;
    v_new_balance NUMERIC(18, 6);
    v_ledger_id   BIGINT;
BEGIN
    SELECT * INTO v_allocation FROM allocations WHERE id = p_allocation_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Allocation not found: id=%', p_allocation_id; END IF;
    IF v_allocation.account_id <> p_account_id THEN
        RAISE EXCEPTION 'Allocation id=% does not belong to account_id=%', p_allocation_id, p_account_id;
    END IF;
    IF v_allocation.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Allocation id=% is not active (status=%)', p_allocation_id, v_allocation.status;
    END IF;
    SELECT * INTO v_account FROM accounts WHERE id = p_account_id FOR UPDATE;
    UPDATE allocations SET status = 'DEALLOCATED', deallocated_at = NOW() WHERE id = p_allocation_id;
    v_new_balance := v_account.balance_grams + v_allocation.amount_grams;
    UPDATE accounts SET
        balance_grams   = v_new_balance,
        allocated_grams = allocated_grams - v_allocation.amount_grams
    WHERE id = p_account_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (p_account_id, 'DEALLOCATE', v_allocation.amount_grams, v_new_balance, p_allocation_id, p_notes)
    RETURNING id INTO v_ledger_id;
    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- credit_yield  (legacy single-user yield)
CREATE OR REPLACE FUNCTION credit_yield(
    p_allocation_id INTEGER,
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_allocation  allocations%ROWTYPE;
    v_account     accounts%ROWTYPE;
    v_yield       NUMERIC(18, 6);
    v_new_balance NUMERIC(18, 6);
    v_ledger_id   BIGINT;
BEGIN
    SELECT * INTO v_allocation FROM allocations WHERE id = p_allocation_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Allocation not found: id=%', p_allocation_id; END IF;
    IF v_allocation.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Cannot credit yield on non-active allocation: id=%, status=%',
            p_allocation_id, v_allocation.status;
    END IF;
    v_yield := v_allocation.amount_grams * v_allocation.yield_rate_bps / 10000.0;
    IF v_yield <= 0 THEN
        RAISE EXCEPTION 'Yield amount must be positive (amount=%, rate_bps=%). Set a non-zero yield rate.',
            v_allocation.amount_grams, v_allocation.yield_rate_bps;
    END IF;
    SELECT * INTO v_account FROM accounts WHERE id = v_allocation.account_id FOR UPDATE;
    v_new_balance := v_account.balance_grams + v_yield;
    UPDATE accounts SET
        balance_grams         = v_new_balance,
        total_deposited_grams = total_deposited_grams + v_yield
    WHERE id = v_allocation.account_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (v_allocation.account_id, 'YIELD_CREDIT', v_yield, v_new_balance, p_allocation_id,
            COALESCE(p_notes, format('Yield credit for allocation %s at %s bps', p_allocation_id, v_allocation.yield_rate_bps)))
    RETURNING id INTO v_ledger_id;
    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- open_lease_deal  (vault → counterparty)
CREATE OR REPLACE FUNCTION open_lease_deal(
    p_amount_grams    NUMERIC(18, 6),
    p_counterparty_id INTEGER,
    p_yield_rate_bps  INTEGER,
    p_maturity_date   DATE         DEFAULT NULL,
    p_deal_reference  VARCHAR(128) DEFAULT NULL,
    p_notes           TEXT         DEFAULT NULL
)
RETURNS TABLE(allocation_id INTEGER, deal_id INTEGER, ledger_event_id BIGINT) AS $$
DECLARE
    v_sys_id      INTEGER;
    v_sys_account accounts%ROWTYPE;
    v_alloc_id    INTEGER;
    v_deal_id     INTEGER;
    v_ledger_id   BIGINT;
    v_new_balance NUMERIC(18, 6);
    v_min_amount  NUMERIC(18, 6);
BEGIN
    IF p_amount_grams <= 0 THEN RAISE EXCEPTION 'Lease amount must be positive, got: %', p_amount_grams; END IF;
    IF p_yield_rate_bps < 0 OR p_yield_rate_bps > 10000 THEN
        RAISE EXCEPTION 'yield_rate_bps must be 0–10000, got: %', p_yield_rate_bps;
    END IF;
    SELECT value::NUMERIC INTO v_min_amount FROM system_config WHERE key = 'min_deal_amount_grams';
    IF p_amount_grams < COALESCE(v_min_amount, 1.0) THEN
        RAISE EXCEPTION 'Amount % is below minimum deal size (%g)', p_amount_grams, v_min_amount;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM counterparties WHERE id = p_counterparty_id AND is_active = TRUE) THEN
        RAISE EXCEPTION 'Counterparty id=% not found or inactive', p_counterparty_id;
    END IF;
    v_sys_id := get_system_account_id();
    SELECT * INTO v_sys_account FROM accounts WHERE id = v_sys_id FOR UPDATE;
    IF v_sys_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION 'Insufficient vault gold: requested=% available=%',
            p_amount_grams, v_sys_account.balance_grams;
    END IF;
    INSERT INTO allocations (account_id, allocation_type, status, amount_grams, yield_rate_bps, is_pooled, maturity_date, notes)
    VALUES (v_sys_id, 'LEASING', 'ACTIVE', p_amount_grams, p_yield_rate_bps, TRUE, p_maturity_date, p_notes)
    RETURNING id INTO v_alloc_id;
    INSERT INTO allocation_deals (allocation_id, counterparty_id, amount_grams, yield_rate_bps, start_date, maturity_date, deal_reference, notes)
    VALUES (v_alloc_id, p_counterparty_id, p_amount_grams, p_yield_rate_bps, CURRENT_DATE, p_maturity_date, p_deal_reference, p_notes)
    RETURNING id INTO v_deal_id;
    v_new_balance := v_sys_account.balance_grams - p_amount_grams;
    UPDATE accounts SET
        balance_grams   = v_new_balance,
        allocated_grams = allocated_grams + p_amount_grams
    WHERE id = v_sys_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
    VALUES (v_sys_id, 'ALLOCATE', p_amount_grams, v_new_balance, v_alloc_id, v_deal_id,
            COALESCE(p_notes, format('Lease opened: counterparty_id=%s deal_id=%s', p_counterparty_id, v_deal_id)))
    RETURNING id INTO v_ledger_id;
    RETURN QUERY SELECT v_alloc_id, v_deal_id, v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- open_collateral_lock  (vault → internal lock, no counterparty)
CREATE OR REPLACE FUNCTION open_collateral_lock(
    p_amount_grams   NUMERIC(18, 6),
    p_yield_rate_bps INTEGER,
    p_maturity_date  DATE DEFAULT NULL,
    p_notes          TEXT DEFAULT NULL
)
RETURNS TABLE(allocation_id INTEGER, ledger_event_id BIGINT) AS $$
DECLARE
    v_sys_id      INTEGER;
    v_sys_account accounts%ROWTYPE;
    v_alloc_id    INTEGER;
    v_ledger_id   BIGINT;
    v_new_balance NUMERIC(18, 6);
BEGIN
    IF p_amount_grams <= 0 THEN RAISE EXCEPTION 'Collateral amount must be positive, got: %', p_amount_grams; END IF;
    IF p_yield_rate_bps < 0 THEN RAISE EXCEPTION 'yield_rate_bps cannot be negative, got: %', p_yield_rate_bps; END IF;
    v_sys_id := get_system_account_id();
    SELECT * INTO v_sys_account FROM accounts WHERE id = v_sys_id FOR UPDATE;
    IF v_sys_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION 'Insufficient vault gold for collateral lock: requested=% available=%',
            p_amount_grams, v_sys_account.balance_grams;
    END IF;
    INSERT INTO allocations (account_id, allocation_type, status, amount_grams, yield_rate_bps, is_pooled, maturity_date, notes)
    VALUES (v_sys_id, 'COLLATERAL_LOCK', 'ACTIVE', p_amount_grams, p_yield_rate_bps, TRUE, p_maturity_date, p_notes)
    RETURNING id INTO v_alloc_id;
    v_new_balance := v_sys_account.balance_grams - p_amount_grams;
    UPDATE accounts SET
        balance_grams   = v_new_balance,
        allocated_grams = allocated_grams + p_amount_grams
    WHERE id = v_sys_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (v_sys_id, 'ALLOCATE', p_amount_grams, v_new_balance, v_alloc_id,
            COALESCE(p_notes, format('Collateral lock opened: allocation_id=%s', v_alloc_id)))
    RETURNING id INTO v_ledger_id;
    RETURN QUERY SELECT v_alloc_id, v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- close_deal  (counterparty → vault + yield earned)
CREATE OR REPLACE FUNCTION close_deal(
    p_deal_id    INTEGER,
    p_is_default BOOLEAN DEFAULT FALSE,
    p_notes      TEXT    DEFAULT NULL
)
RETURNS TABLE(yield_event_id INTEGER, gross_yield_grams NUMERIC(18,6), ledger_close_id BIGINT, ledger_yield_id BIGINT) AS $$
DECLARE
    v_deal          allocation_deals%ROWTYPE;
    v_allocation    allocations%ROWTYPE;
    v_sys_id        INTEGER;
    v_sys_account   accounts%ROWTYPE;
    v_days_held     INTEGER;
    v_gross_yield   NUMERIC(18, 6);
    v_fee_bps       INTEGER;
    v_fee_grams     NUMERIC(18, 6);
    v_distributable NUMERIC(18, 6);
    v_total_user_bal NUMERIC(18, 6);
    v_new_balance   NUMERIC(18, 6);
    v_close_status  deal_status;
    v_lc_id         BIGINT;
    v_ly_id         BIGINT;
    v_ye_id         INTEGER;
BEGIN
    SELECT * INTO v_deal FROM allocation_deals WHERE id = p_deal_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deal not found: id=%', p_deal_id; END IF;
    IF v_deal.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Deal id=% is not ACTIVE (status=%). Cannot close.', p_deal_id, v_deal.status;
    END IF;
    SELECT * INTO v_allocation FROM allocations WHERE id = v_deal.allocation_id FOR UPDATE;
    v_sys_id := get_system_account_id();
    SELECT * INTO v_sys_account FROM accounts WHERE id = v_sys_id FOR UPDATE;
    v_days_held := GREATEST(CURRENT_DATE - v_deal.start_date, 1);
    IF p_is_default THEN
        v_gross_yield := 0;
        v_close_status := 'DEFAULTED';
    ELSE
        v_gross_yield := ROUND(v_deal.amount_grams * v_deal.yield_rate_bps / 10000.0 * v_days_held / 365.0, 6);
        v_close_status := 'MATURED';
    END IF;
    v_fee_bps       := get_system_fee_bps();
    v_fee_grams     := ROUND(v_gross_yield * v_fee_bps / 10000.0, 6);
    v_distributable := v_gross_yield - v_fee_grams;
    -- Principal return
    v_new_balance := v_sys_account.balance_grams + v_deal.amount_grams;
    UPDATE accounts SET balance_grams = v_new_balance, allocated_grams = allocated_grams - v_deal.amount_grams WHERE id = v_sys_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
    VALUES (v_sys_id, 'DEAL_CLOSE', v_deal.amount_grams, v_new_balance, v_allocation.id, p_deal_id,
            COALESCE(p_notes, format('Deal %s closed: principal returned, days_held=%s', p_deal_id, v_days_held)))
    RETURNING id INTO v_lc_id;
    -- Yield income
    v_ly_id := NULL;
    IF v_gross_yield > 0 THEN
        v_new_balance := v_new_balance + v_gross_yield;
        UPDATE accounts SET balance_grams = v_new_balance, total_deposited_grams = total_deposited_grams + v_gross_yield WHERE id = v_sys_id;
        INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
        VALUES (v_sys_id, 'YIELD_EARNED', v_gross_yield, v_new_balance, v_allocation.id, p_deal_id,
                format('Yield earned: %sg gross, %s bps fee, %sg distributable', v_gross_yield, v_fee_bps, v_distributable))
        RETURNING id INTO v_ly_id;
        UPDATE system_state SET system_profit_grams = system_profit_grams + v_fee_grams, last_updated_at = NOW() WHERE id = 1;
    END IF;
    UPDATE allocations SET status = 'DEALLOCATED', deallocated_at = NOW(), closed_at = NOW() WHERE id = v_allocation.id;
    UPDATE allocation_deals SET status = v_close_status, closed_at = NOW() WHERE id = p_deal_id;
    SELECT COALESCE(SUM(a.balance_grams), 0) INTO v_total_user_bal
    FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username <> '__system__' AND a.balance_grams > 0;
    INSERT INTO yield_events (deal_id, allocation_id, gross_yield_grams, system_fee_grams, distributable_yield_grams, system_fee_bps, total_user_balance_snapshot, distribution_completed)
    VALUES (p_deal_id, v_allocation.id, GREATEST(v_gross_yield,0), v_fee_grams, GREATEST(v_distributable,0), v_fee_bps, v_total_user_bal, FALSE)
    RETURNING id INTO v_ye_id;
    RETURN QUERY SELECT v_ye_id, v_gross_yield, v_lc_id, v_ly_id;
END;
$$ LANGUAGE plpgsql;

-- close_collateral_lock  (internal lock → vault + fixed yield)
CREATE OR REPLACE FUNCTION close_collateral_lock(
    p_allocation_id INTEGER,
    p_notes         TEXT DEFAULT NULL
)
RETURNS TABLE(yield_event_id INTEGER, gross_yield_grams NUMERIC(18,6), ledger_dealloc_id BIGINT, ledger_yield_id BIGINT) AS $$
DECLARE
    v_allocation     allocations%ROWTYPE;
    v_sys_id         INTEGER;
    v_sys_account    accounts%ROWTYPE;
    v_days_held      INTEGER;
    v_gross_yield    NUMERIC(18, 6);
    v_fee_bps        INTEGER;
    v_fee_grams      NUMERIC(18, 6);
    v_distributable  NUMERIC(18, 6);
    v_total_user_bal NUMERIC(18, 6);
    v_new_balance    NUMERIC(18, 6);
    v_ld_id          BIGINT;
    v_ly_id          BIGINT;
    v_ye_id          INTEGER;
BEGIN
    SELECT * INTO v_allocation FROM allocations WHERE id = p_allocation_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Allocation not found: id=%', p_allocation_id; END IF;
    IF v_allocation.allocation_type <> 'COLLATERAL_LOCK' THEN
        RAISE EXCEPTION 'Allocation id=% is not COLLATERAL_LOCK (type=%). Use close_deal() for leases.',
            p_allocation_id, v_allocation.allocation_type;
    END IF;
    IF v_allocation.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Allocation id=% is not ACTIVE (status=%).', p_allocation_id, v_allocation.status;
    END IF;
    v_sys_id := get_system_account_id();
    SELECT * INTO v_sys_account FROM accounts WHERE id = v_sys_id FOR UPDATE;
    v_days_held   := GREATEST(CURRENT_DATE - v_allocation.allocated_at::DATE, 1);
    v_gross_yield := ROUND(v_allocation.amount_grams * v_allocation.yield_rate_bps / 10000.0 * v_days_held / 365.0, 6);
    v_fee_bps     := get_system_fee_bps();
    v_fee_grams   := ROUND(v_gross_yield * v_fee_bps / 10000.0, 6);
    v_distributable := v_gross_yield - v_fee_grams;
    -- Principal return
    v_new_balance := v_sys_account.balance_grams + v_allocation.amount_grams;
    UPDATE accounts SET balance_grams = v_new_balance, allocated_grams = allocated_grams - v_allocation.amount_grams WHERE id = v_sys_id;
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (v_sys_id, 'DEALLOCATE', v_allocation.amount_grams, v_new_balance, p_allocation_id,
            COALESCE(p_notes, format('Collateral lock closed: allocation_id=%s', p_allocation_id)))
    RETURNING id INTO v_ld_id;
    -- Yield income
    v_ly_id := NULL;
    IF v_gross_yield > 0 THEN
        v_new_balance := v_new_balance + v_gross_yield;
        UPDATE accounts SET balance_grams = v_new_balance, total_deposited_grams = total_deposited_grams + v_gross_yield WHERE id = v_sys_id;
        INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
        VALUES (v_sys_id, 'YIELD_EARNED', v_gross_yield, v_new_balance, p_allocation_id,
                format('Collateral yield: %sg gross over %s days', v_gross_yield, v_days_held))
        RETURNING id INTO v_ly_id;
        UPDATE system_state SET system_profit_grams = system_profit_grams + v_fee_grams, last_updated_at = NOW() WHERE id = 1;
    END IF;
    UPDATE allocations SET status = 'DEALLOCATED', deallocated_at = NOW(), closed_at = NOW() WHERE id = p_allocation_id;
    SELECT COALESCE(SUM(a.balance_grams), 0) INTO v_total_user_bal
    FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username <> '__system__' AND a.balance_grams > 0;
    INSERT INTO yield_events (deal_id, allocation_id, gross_yield_grams, system_fee_grams, distributable_yield_grams, system_fee_bps, total_user_balance_snapshot, distribution_completed)
    VALUES (NULL, p_allocation_id, GREATEST(v_gross_yield,0), v_fee_grams, GREATEST(v_distributable,0), v_fee_bps, v_total_user_bal, FALSE)
    RETURNING id INTO v_ye_id;
    RETURN QUERY SELECT v_ye_id, v_gross_yield, v_ld_id, v_ly_id;
END;
$$ LANGUAGE plpgsql;

-- distribute_yield
CREATE OR REPLACE FUNCTION distribute_yield(p_yield_event_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    v_ye              yield_events%ROWTYPE;
    v_user_accounts   RECORD;
    v_total_balance   NUMERIC(18, 6);
    v_share           NUMERIC(18, 10);
    v_user_yield      NUMERIC(18, 6);
    v_new_balance     NUMERIC(18, 6);
    v_ledger_id       BIGINT;
    v_users_credited  INTEGER := 0;
    v_sum_distributed NUMERIC(18, 6) := 0;
BEGIN
    SELECT * INTO v_ye FROM yield_events WHERE id = p_yield_event_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'yield_event not found: id=%', p_yield_event_id; END IF;
    IF v_ye.distribution_completed THEN
        RAISE EXCEPTION 'yield_event id=% has already been distributed.', p_yield_event_id;
    END IF;
    IF v_ye.distributable_yield_grams <= 0 THEN
        UPDATE yield_events SET distribution_completed = TRUE, distributed_at = NOW() WHERE id = p_yield_event_id;
        RETURN 0;
    END IF;
    SELECT COALESCE(SUM(a.balance_grams), 0) INTO v_total_balance
    FROM accounts a JOIN users u ON u.id = a.user_id
    WHERE u.username <> '__system__' AND a.balance_grams > 0;
    IF v_total_balance <= 0 THEN
        RAISE EXCEPTION 'No user balances found to distribute yield to.';
    END IF;
    FOR v_user_accounts IN
        SELECT a.id AS account_id, a.balance_grams, u.username
        FROM accounts a JOIN users u ON u.id = a.user_id
        WHERE u.username <> '__system__' AND a.balance_grams > 0
        ORDER BY a.id
        FOR UPDATE OF a
    LOOP
        v_share      := v_user_accounts.balance_grams / v_total_balance;
        v_user_yield := ROUND(v_ye.distributable_yield_grams * v_share, 6);
        CONTINUE WHEN v_user_yield <= 0;
        v_new_balance := v_user_accounts.balance_grams + v_user_yield;
        UPDATE accounts SET
            balance_grams         = v_new_balance,
            total_deposited_grams = total_deposited_grams + v_user_yield
        WHERE id = v_user_accounts.account_id;
        INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
        VALUES (v_user_accounts.account_id, 'YIELD_DISTRIBUTED', v_user_yield, v_new_balance,
                format('yield_event_id=%s share=%.6f', p_yield_event_id, v_share))
        RETURNING id INTO v_ledger_id;
        INSERT INTO yield_distributions (yield_event_id, account_id, user_balance_snapshot, share_fraction, yield_grams, ledger_event_id)
        VALUES (p_yield_event_id, v_user_accounts.account_id, v_user_accounts.balance_grams, v_share, v_user_yield, v_ledger_id);
        v_sum_distributed := v_sum_distributed + v_user_yield;
        v_users_credited  := v_users_credited + 1;
    END LOOP;
    IF ABS(v_sum_distributed - v_ye.distributable_yield_grams) > 0.01 THEN
        RAISE EXCEPTION 'Yield distribution rounding slack too large: expected=% distributed=% slack=%',
            v_ye.distributable_yield_grams, v_sum_distributed,
            v_ye.distributable_yield_grams - v_sum_distributed;
    END IF;
    UPDATE yield_events SET distribution_completed = TRUE, distributed_at = NOW() WHERE id = p_yield_event_id;
    RETURN v_users_credited;
END;
$$ LANGUAGE plpgsql;

-- Audit / reconstruction functions
CREATE OR REPLACE FUNCTION get_account_balance_from_ledger(p_account_id INTEGER)
RETURNS NUMERIC(18, 6) AS $$
BEGIN
    RETURN COALESCE((
        SELECT SUM(CASE
            WHEN event_type IN ('DEPOSIT','YIELD_CREDIT','YIELD_EARNED','DEAL_CLOSE','YIELD_DISTRIBUTED') THEN  amount_grams
            WHEN event_type IN ('WITHDRAW','ALLOCATE')                                                   THEN -amount_grams
            WHEN event_type = 'DEALLOCATE'                                                               THEN  amount_grams
            ELSE 0
        END)
        FROM ledger_events WHERE account_id = p_account_id
    ), 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION verify_ledger_consistency(p_account_id INTEGER)
RETURNS TABLE(
    account_id       INTEGER,
    stored_balance   NUMERIC(18,6),
    ledger_balance   NUMERIC(18,6),
    stored_allocated NUMERIC(18,6),
    ledger_allocated NUMERIC(18,6),
    is_consistent    BOOLEAN
) AS $$
DECLARE
    v_stored_bal   NUMERIC(18,6);
    v_stored_alloc NUMERIC(18,6);
    v_ledger_bal   NUMERIC(18,6);
    v_ledger_alloc NUMERIC(18,6);
BEGIN
    SELECT a.balance_grams, a.allocated_grams INTO v_stored_bal, v_stored_alloc
    FROM accounts a WHERE a.id = p_account_id;
    v_ledger_bal   := get_account_balance_from_ledger(p_account_id);
    SELECT COALESCE(SUM(amount_grams), 0) INTO v_ledger_alloc
    FROM allocations WHERE account_id = p_account_id AND status = 'ACTIVE';
    RETURN QUERY SELECT p_account_id, v_stored_bal, v_ledger_bal, v_stored_alloc, v_ledger_alloc,
        (ABS(v_stored_bal - v_ledger_bal) < 0.001 AND ABS(v_stored_alloc - v_ledger_alloc) < 0.001);
END;
$$ LANGUAGE plpgsql;
