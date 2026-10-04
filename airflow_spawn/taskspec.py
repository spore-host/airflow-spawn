"""Build a spawn TaskSpec for an Airflow task and parse its CompletionRecord.

airflow-spawn no longer orchestrates launch/staging/completion itself — it shells
out to ``spawn task run``, which owns S3 staging, the container run, sizing
(truffle), the scoped IAM profile, and the durable completion record. This module
is the translation layer: it maps the operator's ``command`` + resources to the
TaskSpec JSON shape spawn expects, and reads the CompletionRecord back. Pure (no
I/O, no AWS), unit-tested without a cluster.

TaskSpec contract (spore-host/spawn pkg/taskproto): {task_id, command []string,
container?, resources{cpu,memory_gib,gpus,architecture,families,purchase,...},
inputs[]{source,destination}, outputs[]{source,destination},
lifecycle{ttl,on_complete}, env{}}. Manifests copy s3://<->local; a trailing
slash on the source means recursive.

Unlike cwl-spawn/miniwdl-spawn, an Airflow task carries no pre-staged input tree:
the operator hands us a raw shell ``command`` (file-level input is the command's
own responsibility, via ``aws s3 cp`` — mirroring how ``EcsRunTaskOperator``
leaves container I/O to the container). So the TaskSpec has no input manifest; it
runs the command in a job dir and syncs that dir back so ``stdout.txt`` /
``stderr.txt`` and any files the command wrote land under ``workdir_s3``.
"""

from __future__ import annotations

import json
import logging
import re
import shlex

# Family prefix of an instance type, e.g. "c7i" from "c7i.4xlarge".
_FAMILY_RE = re.compile(r"^([a-z][a-z0-9]*?[0-9]+[a-z]*)\.")


def build_command_string(command: str, job_dir: str) -> str:
    """Wrap the operator's shell ``command`` so it runs in ``job_dir`` with
    stdout/stderr captured to files there (which the output manifest then syncs
    back). Pure.

    We ``mkdir -p`` the job dir first (spawn's stage-in doesn't create it — there
    is no input manifest), pre-create the redirect targets, then run the command
    with ``> stdout.txt 2> stderr.txt``. Not ``set -e`` at this level — the outer
    spawn wrapper deliberately survives a failing command to still write the
    completion record.
    """
    jd = job_dir.rstrip("/")
    return (
        f"mkdir -p {shlex.quote(jd)} "
        f"&& cd {shlex.quote(jd)} "
        f"&& : > stdout.txt && : > stderr.txt "
        f"&& ( {command.rstrip()} ) > stdout.txt 2> stderr.txt"
    )


def instance_type_family(instance_type: str | None) -> str | None:
    """Extract the family prefix from an instance type ("c7i" from "c7i.4xlarge"),
    or None. Maps the operator's ``instance_type`` onto TaskSpec
    ``resources.families`` — spawn has no exact instance-type pin, so it steers
    the family and spawn's sizer picks the cheapest fit within it. Lossy: it does
    NOT pin the exact size."""
    if not instance_type:
        return None
    m = _FAMILY_RE.match(instance_type.strip())
    return m.group(1) if m else None


def _lifecycle(ttl: str, on_complete: str, cost_limit: float | None) -> dict:
    """The lifecycle block, with cost_limit included only when set (#12).

    TTL bounds a task in TIME, not money, and spored enforces the two
    INDEPENDENTLY — first limit to fire wins — so a cost cap is a genuine second
    belt. Without it the only ceiling is the TTL, defaulting to 4h, so a DAG
    fanning out N tasks has a worst case of N x 4h x the instance rate.

    The failure it catches is a task that HANGS rather than fails: it produces no
    error for Airflow to retry or fail on, so it bills until the TTL expires.
    Omitted when unset so spawn's own default still applies.
    """
    lifecycle: dict = {"ttl": ttl, "on_complete": on_complete}
    if cost_limit is None or cost_limit == "":
        return lifecycle
    try:
        value = float(cost_limit)
    except (TypeError, ValueError):
        # cost_limit is a template_field, so a rendered Jinja expression arrives as
        # a string and a bad one must not fail the task at submit time. Degrade to
        # "bounded by TTL only" — the previous behaviour — with a warning.
        logging.getLogger("airflow.task").warning(
            "airflow-spawn: ignoring non-numeric cost_limit %r; "
            "the task will be bounded by TTL only", cost_limit
        )
        return lifecycle
    if value > 0:
        lifecycle["cost_limit"] = value
    return lifecycle


def build_task_spec(
    *,
    task_id: str,
    command: str,
    job_dir: str,
    workdir_s3: str,
    cpus: int | None = None,
    memory_gib: float | None = None,
    instance_hint: str | None = None,
    spot: bool = False,
    ttl: str = "4h",
    on_complete: str = "terminate",
    cost_limit: float | None = None,
    container: str | None = None,
) -> dict:
    """Build the TaskSpec dict for one Airflow task. Pure.

    ``workdir_s3`` is the S3 prefix where results (``stdout.txt``/``stderr.txt`` +
    any files the command wrote) are synced back. No input manifest — the command
    stages its own inputs. The job dir is synced up to ``workdir_s3`` after the
    command runs.
    """
    work_dst = workdir_s3 if workdir_s3.endswith("/") else workdir_s3 + "/"
    jd = job_dir.rstrip("/")

    inner = build_command_string(command, jd)
    argv = ["/bin/bash", "-lc", inner]

    resources: dict = {}
    if cpus and int(cpus) > 0:
        resources["cpu"] = int(cpus)
    if memory_gib and float(memory_gib) > 0:
        resources["memory_gib"] = float(memory_gib)
    fam = instance_type_family(instance_hint)
    if fam:
        resources["families"] = [fam]
    if spot:
        resources["purchase"] = "spot"
        resources["fallback"] = "on_demand"

    spec: dict = {
        "task_id": task_id,
        "command": argv,
        "resources": resources,
        # No inputs — the command fetches its own. Sync the job dir back so
        # stdout/stderr and any outputs land under workdir_s3. Trailing slash on
        # the source ⇒ recursive.
        "outputs": [{"source": jd + "/", "destination": work_dst}],
        "lifecycle": _lifecycle(ttl, on_complete, cost_limit),
    }

    # spec.container routes the task through spawn's existing container path —
    # Docker installed on demand, digest pull, private-ECR auth, GPU flags —
    # instead of a bare AL2023 host where the tool has to already be present
    # (#13). Airflow has no universal per-task image directive the way Nextflow
    # and WDL/CWL do, so this comes from an explicit operator argument. It is also
    # what makes a run's software identifiable, hence reproducible.
    if container and container.strip():
        spec["container"] = container.strip()
    return spec


# ---- completion, from `spawn task status --check-complete` / -o json ----------

def check_complete_to_status(returncode: int) -> str | None:
    """Map ``spawn task status --check-complete`` exit code to a status.

    spawn's contract: 0=completed, 1=failed, 2=running, 3=error. Returns
    "completed"/"failed" on 0/1, None on 2 (not done — poll again), and RAISES on
    3 (spawn couldn't determine status) or any unrecognized code."""
    if returncode == 0:
        return "completed"
    if returncode == 1:
        return "failed"
    if returncode == 2:
        return None
    raise RuntimeError(
        f"`spawn task status --check-complete` returned error/unknown code {returncode}"
    )


def parse_completion_record(stdout: str) -> dict:
    """Parse the CompletionRecord JSON emitted by ``spawn task status <id> -o
    json`` (or ``spawn task run --wait -o json``). Returns the dict; raises on
    invalid JSON. Callers read ``exit_code`` (int) and ``state``."""
    rec = json.loads(stdout)
    if not isinstance(rec, dict):
        raise RuntimeError("completion record is not a JSON object")
    return rec
