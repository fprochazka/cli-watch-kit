from __future__ import annotations

import pytest

from cli_watch_kit.budget import Budget, DimensionSpec

NOW = 1_000_000.0


def test_paces_on_remaining_over_time_to_reset_capped_by_the_ceiling() -> None:
    budget = Budget({"requests": DimensionSpec()})
    budget.observe("requests", NOW, remaining=600, reset_at=NOW + 600, ceiling_per_second=8.0)
    # Half of 600 requests over 600 s is 0.5/s, below the ceiling of 8 * 0.5 = 4/s.
    assert budget.dimension("requests").rate(NOW) == pytest.approx(0.5)
    budget.observe("requests", NOW, remaining=14000, reset_at=NOW + 600)
    assert budget.dimension("requests").rate(NOW) == pytest.approx(4.0)


def test_fixed_rate_without_readings() -> None:
    budget = Budget({"requests": DimensionSpec(per_minute=60)})
    assert budget.dimension("requests").rate(NOW) == pytest.approx(1.0)
    budget.observe("requests", NOW)
    assert budget.dimension("requests").remaining is None
    assert not budget.dimension("requests").headers_seen


def test_token_bucket_blocks_and_refills() -> None:
    budget = Budget({"requests": DimensionSpec(per_minute=60, burst=2)})
    assert budget.can_spend({"requests": 1}, NOW)
    budget.charge({"requests": 1}, NOW)
    budget.charge({"requests": 1}, NOW)
    assert not budget.can_spend({"requests": 1}, NOW)
    assert budget.wait_time({"requests": 1}, NOW) == pytest.approx(1.0)
    assert budget.can_spend({"requests": 1}, NOW + 1.0)


def test_429_blocks_the_account_and_backs_off_exponentially() -> None:
    budget = Budget()
    budget.on_rate_limited(30, NOW)
    assert budget.blocked_until == NOW + 30
    assert not budget.can_spend({"requests": 0.1}, NOW + 29)
    budget.on_rate_limited(None, NOW + 31)
    assert budget.blocked_until == NOW + 31 + 10
    budget.on_success()
    assert budget.backoff == 5


def test_stretch_factor_spreads_shortage_evenly() -> None:
    budget = Budget({"requests": DimensionSpec(per_minute=60)})
    assert budget.stretch_factor({"requests": 0.5}, NOW) == 1.0
    assert budget.stretch_factor({"requests": 3.0}, NOW) == pytest.approx(3.0)


def test_two_dimensions_the_scarcer_one_decides() -> None:
    # Plenty of requests, few complexity points: a cheap call runs, an expensive one waits for points.
    budget = Budget(
        {"requests": DimensionSpec(per_minute=600, burst=10), "complexity": DimensionSpec(per_minute=60, burst=100)}
    )
    assert budget.can_spend({"requests": 1, "complexity": 80}, NOW)
    budget.charge({"requests": 1, "complexity": 80}, NOW)
    assert budget.can_spend({"requests": 1, "complexity": 20}, NOW)
    assert not budget.can_spend({"requests": 1, "complexity": 50}, NOW)
    # 30 points missing at 1 point/s; the requests bucket is full again much sooner.
    assert budget.wait_time({"requests": 1, "complexity": 50}, NOW) == pytest.approx(30.0)
    assert budget.can_spend({"requests": 1, "complexity": 50}, NOW + 30)


def test_two_dimensions_stretch_by_the_dimension_furthest_over() -> None:
    budget = Budget({"requests": DimensionSpec(per_minute=60), "complexity": DimensionSpec(per_minute=600)})
    # Requests at half their rate, complexity at four times its rate of 10/s.
    assert budget.stretch_factor({"requests": 0.5, "complexity": 40.0}, NOW) == pytest.approx(4.0)


def test_readings_of_one_dimension_leave_the_other_alone() -> None:
    budget = Budget({"requests": DimensionSpec(per_minute=60), "complexity": DimensionSpec(per_minute=600)})
    budget.observe("complexity", NOW, remaining=3600, reset_at=NOW + 3600)
    assert budget.dimension("complexity").rate(NOW) == pytest.approx(0.5)
    assert budget.dimension("requests").rate(NOW) == pytest.approx(1.0)
    described = budget.describe(NOW)["dimensions"]
    assert described["complexity"]["headers_seen"] and not described["requests"]["headers_seen"]


def test_a_cost_above_the_bucket_waits_for_a_full_bucket_instead_of_forever() -> None:
    budget = Budget({"complexity": DimensionSpec(per_minute=60, burst=10)})
    assert budget.can_spend({"complexity": 50}, NOW)
    budget.charge({"complexity": 50}, NOW)
    assert budget.dimension("complexity").tokens == -40
    assert budget.wait_time({"complexity": 50}, NOW) == pytest.approx(50.0)


def test_an_undeclared_dimension_gets_the_default_spec() -> None:
    budget = Budget({})
    budget.charge({"points": 3}, NOW)
    assert budget.dimension("points").spec == DimensionSpec()
