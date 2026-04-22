-- T1: Ledger immutability
CREATE OR REPLACE FUNCTION fn_ledger_immutable()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Ledger is immutable: % on ledger_events is forbidden. Event ID: %',
        TG_OP, COALESCE(OLD.id::TEXT, 'N/A');
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_no_update
    BEFORE UPDATE ON ledger_events FOR EACH ROW EXECUTE FUNCTION fn_ledger_immutable();

CREATE TRIGGER trg_ledger_no_delete
    BEFORE DELETE ON ledger_events FOR EACH ROW EXECUTE FUNCTION fn_ledger_immutable();

-- T2: Account balance guard
CREATE OR REPLACE FUNCTION fn_account_balance_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.balance_grams < 0 THEN
        RAISE EXCEPTION 'Insufficient funds: account_id=% attempted balance=% (current=%)',
            NEW.id, NEW.balance_grams, OLD.balance_grams;
    END IF;
    IF NEW.allocated_grams < 0 THEN
        RAISE EXCEPTION 'Invalid state: allocated_grams cannot be negative for account_id=%', NEW.id;
    END IF;
    IF NEW.balance_grams + NEW.allocated_grams >
       NEW.total_deposited_grams - NEW.total_withdrawn_grams + 0.000001 THEN
        RAISE EXCEPTION
            'Account invariant violated for account_id=%: balance(%) + allocated(%) > net_deposited(%)',
            NEW.id, NEW.balance_grams, NEW.allocated_grams,
            NEW.total_deposited_grams - NEW.total_withdrawn_grams;
    END IF;
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_account_balance_guard
    BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION fn_account_balance_guard();

-- T3: System state sync — handles ALL event types
CREATE OR REPLACE FUNCTION fn_sync_system_state()
RETURNS TRIGGER AS $$
BEGIN
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
        UPDATE system_state SET
            total_deposited_grams = total_deposited_grams + NEW.amount_grams,
            vault_gold_grams      = vault_gold_grams + NEW.amount_grams,
            last_updated_at       = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'DEAL_CLOSE' THEN
        UPDATE system_state SET
            vault_gold_grams     = vault_gold_grams + NEW.amount_grams,
            allocated_gold_grams = allocated_gold_grams - NEW.amount_grams,
            last_updated_at      = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'YIELD_EARNED' THEN
        UPDATE system_state SET
            total_deposited_grams     = total_deposited_grams + NEW.amount_grams,
            vault_gold_grams          = vault_gold_grams + NEW.amount_grams,
            total_yield_earned_grams  = total_yield_earned_grams + NEW.amount_grams,
            last_updated_at           = NOW()
        WHERE id = 1;

    ELSIF NEW.event_type = 'YIELD_DISTRIBUTED' THEN
        UPDATE system_state SET
            vault_gold_grams                = vault_gold_grams - NEW.amount_grams,
            total_yield_distributed_grams   = total_yield_distributed_grams + NEW.amount_grams,
            last_updated_at                 = NOW()
        WHERE id = 1;
    END IF;

    PERFORM fn_assert_system_invariant();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_system_state
    AFTER INSERT ON ledger_events FOR EACH ROW EXECUTE FUNCTION fn_sync_system_state();

-- Helper: system invariant assertion
CREATE OR REPLACE FUNCTION fn_assert_system_invariant()
RETURNS VOID AS $$
DECLARE
    ss            system_state%ROWTYPE;
    net_deposited NUMERIC(18, 6);
    accounted     NUMERIC(18, 6);
BEGIN
    SELECT * INTO ss FROM system_state WHERE id = 1;
    net_deposited := ss.total_deposited_grams - ss.total_withdrawn_grams;
    accounted     := ss.vault_gold_grams + ss.allocated_gold_grams + ss.pending_withdrawal_grams;
    IF ABS(net_deposited - accounted) > 0.001 THEN
        RAISE EXCEPTION
            'SYSTEM INVARIANT VIOLATED: net_deposited=% != vault(%) + allocated(%) + pending(%) [diff=%]',
            net_deposited, ss.vault_gold_grams, ss.allocated_gold_grams,
            ss.pending_withdrawal_grams, net_deposited - accounted;
    END IF;
    IF ss.total_yield_distributed_grams > ss.total_yield_earned_grams + 0.001 THEN
        RAISE EXCEPTION 'YIELD INVARIANT VIOLATED: distributed(%) > earned(%)',
            ss.total_yield_distributed_grams, ss.total_yield_earned_grams;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- T4: Allocation status guard
CREATE OR REPLACE FUNCTION fn_allocation_status_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status = 'DEALLOCATED' AND NEW.status = 'ACTIVE' THEN
        RAISE EXCEPTION 'Cannot re-activate a deallocated allocation (id=%)', OLD.id;
    END IF;
    IF NEW.amount_grams <> OLD.amount_grams THEN
        RAISE EXCEPTION 'Allocation amount is immutable after creation (id=%)', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_allocation_status_guard
    BEFORE UPDATE ON allocations FOR EACH ROW EXECUTE FUNCTION fn_allocation_status_guard();

-- T5: Prevent account deletion with non-zero balance
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
    BEFORE DELETE ON accounts FOR EACH ROW EXECUTE FUNCTION fn_account_no_delete_with_balance();

-- T6: System state guard — vault/allocated cannot go negative
CREATE OR REPLACE FUNCTION fn_system_state_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.vault_gold_grams < 0 THEN
        RAISE EXCEPTION
            'System invariant violated: vault_gold_grams would go negative (%).',
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
    BEFORE UPDATE ON system_state FOR EACH ROW EXECUTE FUNCTION fn_system_state_guard();

-- T7: Enforce LEASING must have deal, COLLATERAL must not
CREATE OR REPLACE FUNCTION fn_enforce_deal_allocation_type()
RETURNS TRIGGER AS $$
DECLARE
    v_alloc_type    allocation_type;
    v_alloc_amount  NUMERIC(18, 6);
BEGIN
    SELECT allocation_type, amount_grams
    INTO v_alloc_type, v_alloc_amount
    FROM allocations WHERE id = NEW.allocation_id;

    IF v_alloc_type = 'COLLATERAL_LOCK' THEN
        RAISE EXCEPTION
            'COLLATERAL_LOCK allocations (id=%) cannot have external deals. Only LEASING allocations link to counterparties.',
            NEW.allocation_id;
    END IF;
    IF v_alloc_type IS NULL THEN
        RAISE EXCEPTION 'Allocation id=% not found.', NEW.allocation_id;
    END IF;
    IF NEW.amount_grams > v_alloc_amount + 0.000001 THEN
        RAISE EXCEPTION
            'Deal amount (%) cannot exceed allocation amount (%) for allocation_id=%',
            NEW.amount_grams, v_alloc_amount, NEW.allocation_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_deal_allocation_type
    BEFORE INSERT ON allocation_deals FOR EACH ROW EXECUTE FUNCTION fn_enforce_deal_allocation_type();

-- T8: Counterparty exposure limit
CREATE OR REPLACE FUNCTION fn_check_counterparty_exposure()
RETURNS TRIGGER AS $$
DECLARE
    v_cpty             counterparties%ROWTYPE;
    v_current_exposure NUMERIC(18, 6);
    v_new_total        NUMERIC(18, 6);
BEGIN
    SELECT * INTO v_cpty FROM counterparties WHERE id = NEW.counterparty_id FOR SHARE;
    IF NOT v_cpty.is_active THEN
        RAISE EXCEPTION 'Counterparty id=% (%) is not active. Cannot create deal.',
            v_cpty.id, v_cpty.name;
    END IF;
    SELECT COALESCE(SUM(amount_grams), 0) INTO v_current_exposure
    FROM allocation_deals
    WHERE counterparty_id = NEW.counterparty_id
      AND status = 'ACTIVE'
      AND id <> COALESCE(NEW.id, -1);
    v_new_total := v_current_exposure + NEW.amount_grams;
    IF v_cpty.max_exposure_grams > 0 AND v_new_total > v_cpty.max_exposure_grams THEN
        RAISE EXCEPTION
            'Counterparty exposure limit exceeded for % (id=%): current=% + new=% = % > max=%',
            v_cpty.name, v_cpty.id, v_current_exposure,
            NEW.amount_grams, v_new_total, v_cpty.max_exposure_grams;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_counterparty_exposure
    BEFORE INSERT OR UPDATE ON allocation_deals FOR EACH ROW EXECUTE FUNCTION fn_check_counterparty_exposure();

-- T9: Deal status immutability guard
CREATE OR REPLACE FUNCTION fn_deal_status_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IN ('MATURED', 'DEFAULTED', 'CANCELLED') AND NEW.status <> OLD.status THEN
        RAISE EXCEPTION 'Deal id=% is in terminal status (%). Cannot change to %.',
            OLD.id, OLD.status, NEW.status;
    END IF;
    IF NEW.amount_grams <> OLD.amount_grams THEN
        RAISE EXCEPTION 'Deal amount is immutable after creation (deal_id=%).', OLD.id;
    END IF;
    IF NEW.counterparty_id <> OLD.counterparty_id THEN
        RAISE EXCEPTION 'Deal counterparty cannot be changed after creation (deal_id=%).', OLD.id;
    END IF;
    IF NEW.allocation_id <> OLD.allocation_id THEN
        RAISE EXCEPTION 'Deal allocation_id cannot be changed after creation (deal_id=%).', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_deal_status_guard
    BEFORE UPDATE ON allocation_deals FOR EACH ROW EXECUTE FUNCTION fn_deal_status_guard();

-- T10: Yield distribution totals guard
CREATE OR REPLACE FUNCTION fn_yield_distribution_totals_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_sum_distributed NUMERIC(18, 6);
BEGIN
    IF NEW.distribution_completed = TRUE AND OLD.distribution_completed = FALSE THEN
        SELECT COALESCE(SUM(yield_grams), 0) INTO v_sum_distributed
        FROM yield_distributions WHERE yield_event_id = NEW.id;
        IF ABS(v_sum_distributed - NEW.distributable_yield_grams) > 0.001 THEN
            RAISE EXCEPTION
                'Yield distribution totals mismatch for yield_event_id=%: sum=% != distributable=%',
                NEW.id, v_sum_distributed, NEW.distributable_yield_grams;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_yield_distribution_totals_guard
    BEFORE UPDATE ON yield_events FOR EACH ROW EXECUTE FUNCTION fn_yield_distribution_totals_guard();
