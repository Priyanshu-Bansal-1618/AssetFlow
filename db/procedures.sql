-- PROCEDURE: deposit_gold
--
-- Flow:
--   1. Lock account row (FOR UPDATE) — prevents concurrent deposit/withdraw races
--   2. Update account balance and totals
--   3. Insert ledger event (triggers system_state sync automatically)
--
-- Returns: ledger_event id

CREATE OR REPLACE FUNCTION deposit_gold(
    p_account_id    INTEGER,
    p_amount_grams  NUMERIC(18, 6),
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account       accounts%ROWTYPE;
    v_new_balance   NUMERIC(18, 6);
    v_ledger_id     BIGINT;
BEGIN
    -- Validate input
    IF p_amount_grams <= 0 THEN
        RAISE EXCEPTION 'Deposit amount must be positive, got: %', p_amount_grams;
    END IF;

    -- Lock the account row to prevent concurrent modifications
    SELECT * INTO v_account
    FROM accounts
    WHERE id = p_account_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Account not found: id=%', p_account_id;
    END IF;

    v_new_balance := v_account.balance_grams + p_amount_grams;

    -- Update account state
    UPDATE accounts SET
        balance_grams = v_new_balance,
        total_deposited_grams = total_deposited_grams + p_amount_grams
    WHERE id = p_account_id;

    -- Insert ledger event (triggers system_state sync)
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
    VALUES (p_account_id, 'DEPOSIT', p_amount_grams, v_new_balance, p_notes)
    RETURNING id INTO v_ledger_id;

    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE: withdraw_gold
--
-- Flow:
--   1. Lock account row
--   2. Verify sufficient AVAILABLE balance (balance_grams, not including allocated)
--   3. Update account balance
--   4. Insert ledger event
--
-- Returns: ledger_event id
CREATE OR REPLACE FUNCTION withdraw_gold(
    p_account_id    INTEGER,
    p_amount_grams  NUMERIC(18, 6),
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account       accounts%ROWTYPE;
    v_new_balance   NUMERIC(18, 6);
    v_ledger_id     BIGINT;
BEGIN
    IF p_amount_grams <= 0 THEN
        RAISE EXCEPTION 'Withdrawal amount must be positive, got: %', p_amount_grams;
    END IF;

    SELECT * INTO v_account
    FROM accounts
    WHERE id = p_account_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Account not found: id=%', p_account_id;
    END IF;

    -- Only available balance can be withdrawn (not allocated gold)
    IF v_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION
            'Insufficient available balance: requested=% available=% (note: %g is allocated and unavailable)',
            p_amount_grams,
            v_account.balance_grams,
            v_account.allocated_grams;
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

-- PROCEDURE: allocate_gold
--
-- Flow:
--   1. Lock account row
--   2. Verify sufficient available balance
--   3. Create allocation record
--   4. Move balance from available → allocated on account
--   5. Insert ledger event (references allocation id)
--
-- Returns: (ledger_event_id, allocation_id)
CREATE OR REPLACE FUNCTION allocate_gold(
    p_account_id        INTEGER,
    p_amount_grams      NUMERIC(18, 6),
    p_allocation_type   allocation_type,
    p_yield_rate_bps    INTEGER DEFAULT 0,
    p_notes             TEXT DEFAULT NULL
)
RETURNS TABLE(ledger_event_id BIGINT, allocation_id INTEGER) AS $$
DECLARE
    v_account       accounts%ROWTYPE;
    v_alloc_id      INTEGER;
    v_ledger_id     BIGINT;
    v_new_balance   NUMERIC(18, 6);
BEGIN
    IF p_amount_grams <= 0 THEN
        RAISE EXCEPTION 'Allocation amount must be positive, got: %', p_amount_grams;
    END IF;

    IF p_yield_rate_bps < 0 THEN
        RAISE EXCEPTION 'Yield rate cannot be negative, got: %', p_yield_rate_bps;
    END IF;

    SELECT * INTO v_account
    FROM accounts
    WHERE id = p_account_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Account not found: id=%', p_account_id;
    END IF;

    IF v_account.balance_grams < p_amount_grams THEN
        RAISE EXCEPTION
            'Insufficient available balance for allocation: requested=% available=%',
            p_amount_grams, v_account.balance_grams;
    END IF;

    -- Create allocation record first (we need its ID for the ledger)
    INSERT INTO allocations (account_id, allocation_type, amount_grams, yield_rate_bps, notes)
    VALUES (p_account_id, p_allocation_type, p_amount_grams, p_yield_rate_bps, p_notes)
    RETURNING id INTO v_alloc_id;

    -- Move from available balance to allocated
    v_new_balance := v_account.balance_grams - p_amount_grams;

    UPDATE accounts SET
        balance_grams   = v_new_balance,
        allocated_grams = allocated_grams + p_amount_grams
    WHERE id = p_account_id;

    -- Insert ledger event referencing the allocation
    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (p_account_id, 'ALLOCATE', p_amount_grams, v_new_balance, v_alloc_id, p_notes)
    RETURNING id INTO v_ledger_id;

    RETURN QUERY SELECT v_ledger_id, v_alloc_id;
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE: deallocate_gold
--
-- Flow:
--   1. Lock account row
--   2. Verify allocation belongs to account and is ACTIVE
--   3. Mark allocation as DEALLOCATED
--   4. Move balance from allocated → available on account
--   5. Insert ledger event
--
-- Returns: ledger_event_id
CREATE OR REPLACE FUNCTION deallocate_gold(
    p_account_id    INTEGER,
    p_allocation_id INTEGER,
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_account       accounts%ROWTYPE;
    v_allocation    allocations%ROWTYPE;
    v_new_balance   NUMERIC(18, 6);
    v_ledger_id     BIGINT;
BEGIN
    -- Lock allocation row first (lower-numbered lock first to avoid deadlocks)
    SELECT * INTO v_allocation
    FROM allocations
    WHERE id = p_allocation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Allocation not found: id=%', p_allocation_id;
    END IF;

    IF v_allocation.account_id <> p_account_id THEN
        RAISE EXCEPTION
            'Allocation id=% does not belong to account_id=%',
            p_allocation_id, p_account_id;
    END IF;

    IF v_allocation.status <> 'ACTIVE' THEN
        RAISE EXCEPTION
            'Allocation id=% is not active (status=%)',
            p_allocation_id, v_allocation.status;
    END IF;

    -- Lock account row
    SELECT * INTO v_account
    FROM accounts
    WHERE id = p_account_id
    FOR UPDATE;

    -- Mark allocation as deallocated
    UPDATE allocations SET
        status          = 'DEALLOCATED',
        deallocated_at  = NOW()
    WHERE id = p_allocation_id;

    -- Return gold to available balance
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

-- PROCEDURE: credit_yield
--
-- Credits yield earnings to an account based on an active allocation.
-- Yield amount is: (amount_grams * yield_rate_bps) / 10000
--
-- Returns: ledger_event_id
CREATE OR REPLACE FUNCTION credit_yield(
    p_allocation_id INTEGER,
    p_notes         TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_allocation    allocations%ROWTYPE;
    v_account       accounts%ROWTYPE;
    v_yield_amount  NUMERIC(18, 6);
    v_new_balance   NUMERIC(18, 6);
    v_ledger_id     BIGINT;
BEGIN
    SELECT * INTO v_allocation
    FROM allocations
    WHERE id = p_allocation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Allocation not found: id=%', p_allocation_id;
    END IF;

    IF v_allocation.status <> 'ACTIVE' THEN
        RAISE EXCEPTION
            'Cannot credit yield on non-active allocation: id=%, status=%',
            p_allocation_id, v_allocation.status;
    END IF;

    -- Calculate yield: amount * rate_bps / 10000
    v_yield_amount := v_allocation.amount_grams * v_allocation.yield_rate_bps / 10000.0;

    IF v_yield_amount <= 0 THEN
        RAISE EXCEPTION
            'Yield amount must be positive (amount=%, rate_bps=%). Set a non-zero yield rate.',
            v_allocation.amount_grams, v_allocation.yield_rate_bps;
    END IF;

    SELECT * INTO v_account
    FROM accounts
    WHERE id = v_allocation.account_id
    FOR UPDATE;

    v_new_balance := v_account.balance_grams + v_yield_amount;

    UPDATE accounts SET
        balance_grams         = v_new_balance,
        total_deposited_grams = total_deposited_grams + v_yield_amount
    WHERE id = v_allocation.account_id;

    INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
    VALUES (
        v_allocation.account_id,
        'YIELD_CREDIT',
        v_yield_amount,
        v_new_balance,
        p_allocation_id,
        COALESCE(p_notes, format('Yield credit for allocation %s at %s bps', p_allocation_id, v_allocation.yield_rate_bps))
    )
    RETURNING id INTO v_ledger_id;

    RETURN v_ledger_id;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: get_account_balance_from_ledger
-- Reconstructs account balance purely from ledger history.
-- This validates that the derived account.balance_grams is consistent.
CREATE OR REPLACE FUNCTION get_account_balance_from_ledger(p_account_id INTEGER)
RETURNS NUMERIC(18, 6) AS $$
DECLARE
    v_reconstructed NUMERIC(18, 6);
BEGIN
    SELECT
        COALESCE(SUM(
            CASE
                WHEN event_type IN ('DEPOSIT', 'YIELD_CREDIT') THEN  amount_grams
                WHEN event_type IN ('WITHDRAW', 'ALLOCATE')   THEN -amount_grams
                WHEN event_type = 'DEALLOCATE'                THEN  amount_grams
                ELSE 0
            END
        ), 0)
    INTO v_reconstructed
    FROM ledger_events
    WHERE account_id = p_account_id;

    RETURN v_reconstructed;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: verify_ledger_consistency
-- Compares reconstructed balance from ledger against stored account balance.
-- Used for auditing. Returns TRUE if consistent.
CREATE OR REPLACE FUNCTION verify_ledger_consistency(p_account_id INTEGER)
RETURNS TABLE(
    account_id          INTEGER,
    stored_balance      NUMERIC(18, 6),
    ledger_balance      NUMERIC(18, 6),
    stored_allocated    NUMERIC(18, 6),
    ledger_allocated    NUMERIC(18, 6),
    is_consistent       BOOLEAN
) AS $$
DECLARE
    v_stored_balance    NUMERIC(18, 6);
    v_stored_allocated  NUMERIC(18, 6);
    v_ledger_balance    NUMERIC(18, 6);
    v_ledger_allocated  NUMERIC(18, 6);
BEGIN
    SELECT a.balance_grams, a.allocated_grams
    INTO v_stored_balance, v_stored_allocated
    FROM accounts a
    WHERE a.id = p_account_id;

    v_ledger_balance := get_account_balance_from_ledger(p_account_id);

    -- Allocated grams from active allocations
    SELECT COALESCE(SUM(amount_grams), 0)
    INTO v_ledger_allocated
    FROM allocations
    WHERE account_id = p_account_id
      AND status = 'ACTIVE';

    RETURN QUERY SELECT
        p_account_id,
        v_stored_balance,
        v_ledger_balance,
        v_stored_allocated,
        v_ledger_allocated,
        (ABS(v_stored_balance - v_ledger_balance) < 0.001
         AND ABS(v_stored_allocated - v_ledger_allocated) < 0.001);
END;
$$ LANGUAGE plpgsql;
