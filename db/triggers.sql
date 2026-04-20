-- Triggers: Invariant enforcement, ledger immutability, derived state


-- TRIGGER 1: Ledger Immutability
-- The ledger is append-only. Block all UPDATE and DELETE on ledger_events.
CREATE OR REPLACE FUNCTION fn_ledger_immutable()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'Ledger is immutable: % on ledger_events is forbidden. Event ID: %',
        TG_OP,
        COALESCE(OLD.id::TEXT, 'N/A');
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_no_update
    BEFORE UPDATE ON ledger_events
    FOR EACH ROW
    EXECUTE FUNCTION fn_ledger_immutable();

CREATE TRIGGER trg_ledger_no_delete
    BEFORE DELETE ON ledger_events
    FOR EACH ROW
    EXECUTE FUNCTION fn_ledger_immutable();


-- TRIGGER 2: Account Balance Enforcement (Pre-write guard)
-- Before any UPDATE on accounts, verify balance never goes negative.
-- Also keeps updated_at fresh.
CREATE OR REPLACE FUNCTION fn_account_balance_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.balance_grams < 0 THEN
        RAISE EXCEPTION
            'Insufficient funds: account_id=% attempted balance=% (current=%)',
            NEW.id, NEW.balance_grams, OLD.balance_grams;
    END IF;

    IF NEW.allocated_grams < 0 THEN
        RAISE EXCEPTION
            'Invalid state: allocated_grams cannot be negative for account_id=%',
            NEW.id;
    END IF;

    -- Prevent available + allocated from exceeding net deposited
    IF NEW.balance_grams + NEW.allocated_grams >
       NEW.total_deposited_grams - NEW.total_withdrawn_grams + 0.000001 THEN
        RAISE EXCEPTION
            'Account invariant violated for account_id=%: balance(%) + allocated(%) > net_deposited(%)',
            NEW.id,
            NEW.balance_grams,
            NEW.allocated_grams,
            NEW.total_deposited_grams - NEW.total_withdrawn_grams;
    END IF;

    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_account_balance_guard
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION fn_account_balance_guard();


-- TRIGGER 3: System State Sync
-- After every ledger_events INSERT, update system_state automatically.
-- This is the single authoritative source for system-wide totals.
CREATE OR REPLACE FUNCTION fn_sync_system_state()
RETURNS TRIGGER AS $$
BEGIN
    -- Lock the single system_state row to prevent concurrent updates
    PERFORM pg_advisory_xact_lock(1);

    IF NEW.event_type = 'DEPOSIT' THEN
        UPDATE system_state SET
            total_deposited_grams = total_deposited_grams + NEW.amount_grams,
            vault_gold_grams      = vault_gold_grams + NEW.amount_grams,
            last_updated_at       = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'WITHDRAW' THEN
        UPDATE system_state SET
            total_withdrawn_grams = total_withdrawn_grams + NEW.amount_grams,
            vault_gold_grams      = vault_gold_grams - NEW.amount_grams,
            last_updated_at       = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'ALLOCATE' THEN
        UPDATE system_state SET
            vault_gold_grams     = vault_gold_grams - NEW.amount_grams,
            allocated_gold_grams = allocated_gold_grams + NEW.amount_grams,
            last_updated_at      = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'DEALLOCATE' THEN
        UPDATE system_state SET
            vault_gold_grams     = vault_gold_grams + NEW.amount_grams,
            allocated_gold_grams = allocated_gold_grams - NEW.amount_grams,
            last_updated_at      = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'YIELD_CREDIT' THEN
        -- Yield credits add to vault and total_deposited (they are new gold coming in)
        UPDATE system_state SET
            total_deposited_grams = total_deposited_grams + NEW.amount_grams,
            vault_gold_grams      = vault_gold_grams + NEW.amount_grams,
            last_updated_at       = NOW()
        WHERE id = 1;
    END IF;

    -- Verify system invariant immediately after update
    PERFORM fn_assert_system_invariant();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_system_state
    AFTER INSERT ON ledger_events
    FOR EACH ROW
    EXECUTE FUNCTION fn_sync_system_state();


-- HELPER: System Invariant Assertion
-- Called by the system_state sync trigger. Raises if invariant is violated.
CREATE OR REPLACE FUNCTION fn_assert_system_invariant()
RETURNS VOID AS $$
DECLARE
    ss              system_state%ROWTYPE;
    net_deposited   NUMERIC(18, 6);
    accounted       NUMERIC(18, 6);
BEGIN
    SELECT * INTO ss FROM system_state WHERE id = 1;

    net_deposited := ss.total_deposited_grams - ss.total_withdrawn_grams;
    accounted     := ss.vault_gold_grams + ss.allocated_gold_grams + ss.pending_withdrawal_grams;

    IF ABS(net_deposited - accounted) > 0.001 THEN
        RAISE EXCEPTION
            'SYSTEM INVARIANT VIOLATED: net_deposited=% != vault(%) + allocated(%) + pending(%) [diff=%]',
            net_deposited,
            ss.vault_gold_grams,
            ss.allocated_gold_grams,
            ss.pending_withdrawal_grams,
            net_deposited - accounted;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- TRIGGER 4: Allocation Status Guard
-- Prevent re-allocating or double-deallocating an allocation.
CREATE OR REPLACE FUNCTION fn_allocation_status_guard()
RETURNS TRIGGER AS $$
BEGIN
    -- Can only transition ACTIVE → DEALLOCATED, never reverse
    IF OLD.status = 'DEALLOCATED' AND NEW.status = 'ACTIVE' THEN
        RAISE EXCEPTION
            'Cannot re-activate a deallocated allocation (id=%)', OLD.id;
    END IF;

    -- Amount must never change after creation
    IF NEW.amount_grams <> OLD.amount_grams THEN
        RAISE EXCEPTION
            'Allocation amount is immutable after creation (id=%)', OLD.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_allocation_status_guard
    BEFORE UPDATE ON allocations
    FOR EACH ROW
    EXECUTE FUNCTION fn_allocation_status_guard();


-- TRIGGER 5: Prevent Account Deletion if Non-Zero Balance
CREATE OR REPLACE FUNCTION fn_account_no_delete_with_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.balance_grams > 0 OR OLD.allocated_grams > 0 THEN
        RAISE EXCEPTION
            'Cannot delete account_id=% with non-zero balance (balance=%, allocated=%)',
            OLD.id, OLD.balance_grams, OLD.allocated_grams;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_account_no_delete_with_balance
    BEFORE DELETE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION fn_account_no_delete_with_balance();


-- TRIGGER 6: Vault Gold Cannot Go Negative (system_state guard)
CREATE OR REPLACE FUNCTION fn_system_state_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.vault_gold_grams < 0 THEN
        RAISE EXCEPTION
            'System invariant violated: vault_gold_grams would go negative (%). '
            'This indicates over-allocation or over-withdrawal.',
            NEW.vault_gold_grams;
    END IF;

    IF NEW.allocated_gold_grams < 0 THEN
        RAISE EXCEPTION
            'System invariant violated: allocated_gold_grams would go negative (%).',
            NEW.allocated_gold_grams;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_system_state_guard
    BEFORE UPDATE ON system_state
    FOR EACH ROW
    EXECUTE FUNCTION fn_system_state_guard();