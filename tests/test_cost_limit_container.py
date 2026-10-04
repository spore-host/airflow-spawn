"""lifecycle.cost_limit (#12) and spec.container (#13)."""

from __future__ import annotations

from airflow_spawn import taskspec


def _spec(**kw) -> dict:
    base = dict(
        task_id="t1",
        command="echo hi",
        job_dir="/tmp/job",
        workdir_s3="s3://b/runs/x",
    )
    base.update(kw)
    return taskspec.build_task_spec(**base)


# ---- #12: cost_limit --------------------------------------------------------


def test_cost_limit_is_emitted():
    """Second belt: spored enforces TTL and cost independently, first to fire
    wins. Without a cap the only ceiling is the TTL (4h default), so a DAG
    fanning out N tasks has a worst case of N x 4h x the instance rate."""
    assert _spec(cost_limit=0.05)["lifecycle"]["cost_limit"] == 0.05


def test_cost_limit_omitted_when_unset():
    assert "cost_limit" not in _spec()["lifecycle"]
    assert "cost_limit" not in _spec(cost_limit=None)["lifecycle"]


def test_zero_or_negative_is_unset():
    """A zero cap would mean "terminate immediately"."""
    assert "cost_limit" not in _spec(cost_limit=0)["lifecycle"]
    assert "cost_limit" not in _spec(cost_limit=-1)["lifecycle"]


def test_rendered_template_string_is_coerced():
    """cost_limit is a template_field, and a rendered Jinja expression is always a
    STRING — so the float coercion is load-bearing, not defensive."""
    v = _spec(cost_limit="0.25")["lifecycle"]["cost_limit"]
    assert v == 0.25 and isinstance(v, float)
    assert "cost_limit" not in _spec(cost_limit="")["lifecycle"]


def test_bad_template_render_degrades_rather_than_failing_the_task():
    """A template that renders to junk must not fail the task at submit time; it
    degrades to "bounded by TTL only", which is the previous behaviour."""
    assert "cost_limit" not in _spec(cost_limit="{{ unrendered }}")["lifecycle"]
    assert "cost_limit" not in _spec(cost_limit="lots")["lifecycle"]


def test_ttl_and_on_complete_unaffected():
    assert _spec(ttl="30m", cost_limit=1.5)["lifecycle"] == {
        "ttl": "30m",
        "on_complete": "terminate",
        "cost_limit": 1.5,
    }


# ---- #13: container ---------------------------------------------------------


def test_container_is_emitted():
    """Routes the task through spawn's container path — Docker on demand, digest
    pull, private-ECR auth, GPU flags — rather than a bare AL2023 host where the
    tool has to already be present. airflow-spawn was the only adapter with
    neither the field nor a knob for it."""
    img = "quay.io/biocontainers/bwa:0.7.18--he4a0461_1"
    assert _spec(container=img)["container"] == img


def test_container_omitted_when_unset():
    """No container means run on the host — the previous behaviour, still valid
    for a task that needs nothing installed."""
    for spec in (_spec(), _spec(container=None), _spec(container=""), _spec(container="  ")):
        assert "container" not in spec


def test_container_is_stripped():
    """A templated image ref commonly arrives with surrounding whitespace."""
    assert _spec(container="  alpine:3  ")["container"] == "alpine:3"
