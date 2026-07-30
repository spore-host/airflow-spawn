#!/usr/bin/env python3
"""Engine-composition test: real SpawnRunTaskOperator ↔ spawn ↔ Substrate.

Proves the operator drives a task end-to-end against the Substrate AWS emulator
(no real AWS, no cost, no workload execution): real `op.execute()` → real
`spawn task run` (real RunInstances + real `aws s3` staging vs. Substrate) →
poll the completion record → return/raise on the exit code.

Substrate (v0.75.0+) serves the completion record as a seedable, clock-aware
outcome (substrate#360): unseeded ⇒ nominal exit 0 (happy path, no seed); a
POST /v1/spawn/task-completion with a nonzero exit_code drives the failure path.

Standalone: we call the operator's execute() directly (no Airflow scheduler).
Airflow logs a harmless "cannot be called outside of the Task Runner" notice for
a direct execute() — expected, not an error.

Requires on PATH / in env (CI provides): `spawn`, `aws`; AWS_ENDPOINT_URL →
Substrate. Exits nonzero if any assertion fails.
"""
import os
import subprocess
import sys
import urllib.request

ENDPOINT = os.environ["AWS_ENDPOINT_URL"]  # set by the CI job
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
# Blank the named-profile defaults so the SDK uses the static test creds + endpoint.
os.environ["SPAWN_INFRA_PROFILE"] = ""
os.environ["SPAWN_COMPUTE_PROFILE"] = ""

WORKDIR_BUCKET = "airflow-spawn-ci"
WORKDIR_S3 = f"s3://{WORKDIR_BUCKET}/runs"
REGION = "us-east-1"


def fail(msg):
    print(f"COMPOSITION TEST FAILED: {msg}", file=sys.stderr)
    sys.exit(1)


def seed_failure(task_id, exit_code=7):
    body = (
        f'{{"task_id":"{task_id}","exit_code":{exit_code},"state":"failed",'
        f'"started_at":"2026-01-01T00:00:00Z","ended_at":"2026-01-01T00:00:01Z"}}'
    ).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/spawn/task-completion", data=body,
        headers={"content-type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        if r.status != 200:
            fail(f"seed POST returned {r.status}")


def make_op(task_id):
    from airflow_spawn import SpawnRunTaskOperator
    return SpawnRunTaskOperator(
        task_id=task_id,
        command="echo 'composition step ran'",  # a shell string (operator's contract)
        cpus=2, memory_gib=4, region=REGION, ttl="20m",
        workdir_s3=WORKDIR_S3, deferrable=False,
    )


def main():
    print(f"spawn={subprocess.run(['which','spawn'],capture_output=True,text=True).stdout.strip()} "
          f"endpoint={ENDPOINT}")
    # workdir base bucket must exist for staging uploads (spawn creates the results bucket).
    subprocess.run(["aws", "--endpoint-url", ENDPOINT, "s3", "mb", f"s3://{WORKDIR_BUCKET}"],
                   capture_output=True, text=True)

    # The operator's spawn task_id for a standalone execute() is af-<task_id>-1.
    # TEST 1 — happy path: unseeded ⇒ nominal success, execute() returns exit 0.
    # On success _finish returns workdir_s3 (a string) and does NOT raise; on a
    # nonzero exit it raises AirflowException.
    print("=== TEST 1: happy path — unseeded completion ⇒ success ===")
    try:
        result = make_op("t1").execute({})
    except Exception as e:
        fail(f"TEST 1: execute() raised on the happy path ({type(e).__name__}: {e})")
    if not result:
        fail(f"TEST 1: execute() returned falsy {result!r}, expected the workdir on success")
    print(f"TEST 1 PASS: operator dispatched spawn task run; completion exit 0 → returned {result}")

    # TEST 2 — failure path: seed af-t2-1 to exit 7 ⇒ execute() must raise.
    print("=== TEST 2: failure path — seed exit 7 ⇒ operator raises ===")
    seed_failure("af-t2-1", 7)
    try:
        make_op("t2").execute({})
    except Exception as e:  # AirflowException on nonzero exit
        print(f"TEST 2 PASS: seeded exit 7 → operator raised ({type(e).__name__}: {e})")
    else:
        fail("TEST 2: execute() returned normally despite a seeded FAILED completion")

    print("=== COMPOSITION VERIFIED: real SpawnRunTaskOperator ↔ spawn ↔ Substrate ===")


if __name__ == "__main__":
    main()
