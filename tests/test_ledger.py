"""The ledger's guards in isolation: what each one does when the same task's charge arrives twice."""

import sqlite3

import pytest

from taskq.ledger import Ledger


def charge(ledger: Ledger, task_id: str, token: int, guard: str, amount: int | None = 100) -> str:
    return ledger.charge(task_id, "alice", amount, token, f"worker-{token}", guard)  # type: ignore[arg-type]


def test_without_a_guard_a_second_run_charges_again(ledger: Ledger) -> None:
    assert charge(ledger, "t", 1, "none") == "charged"
    assert charge(ledger, "t", 2, "none") == "charged"

    assert ledger.summary().duplicated == {"t": 2}


def test_dedupe_applies_each_task_once(ledger: Ledger) -> None:
    assert charge(ledger, "t", 1, "dedupe") == "charged"
    # Same task again, whichever lease it comes from
    assert charge(ledger, "t", 2, "dedupe") == "duplicate"
    assert charge(ledger, "t", 1, "dedupe") == "duplicate"
    assert charge(ledger, "other", 1, "dedupe") == "charged"

    assert ledger.charges_for("t") == [(1, "worker-1")]


def test_dedupe_marker_rolls_back_with_a_failed_charge(ledger: Ledger) -> None:
    # The charge itself fails (NOT NULL amount), so the whole transaction must roll back...
    with pytest.raises(sqlite3.IntegrityError):
        charge(ledger, "t", 1, "dedupe", amount=None)

    # ...including the "processed" marker, or the retry would be wrongly skipped as a duplicate
    assert charge(ledger, "t", 2, "dedupe") == "charged"
    assert ledger.charges_for("t") == [(2, "worker-2")]


def test_fence_rejects_an_older_token_after_a_newer_one_wrote(ledger: Ledger) -> None:
    assert charge(ledger, "t", 2, "fence") == "charged"
    assert charge(ledger, "t", 1, "fence") == "stale"

    assert ledger.charges_for("t") == [(2, "worker-2")]


def test_fence_lets_both_through_when_the_older_token_wrote_first(ledger: Ledger) -> None:
    # The known limit of fencing: it rejects writes that arrive late, not writes that happen twice
    assert charge(ledger, "t", 1, "fence") == "charged"
    assert charge(ledger, "t", 2, "fence") == "charged"

    assert ledger.summary().duplicated == {"t": 2}
