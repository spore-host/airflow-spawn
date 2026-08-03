# Changelog

All notable changes to **airflow-spawn** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **A pin's version comment can no longer silently misstate what CI runs.**
  `tests/test_ci_hygiene.py` required only that *some* `# vN` comment be present,
  never that it was true. A wrong label is worse than a missing one: it makes a
  major-version jump read as a routine same-line bump. Not hypothetical —
  Dependabot bumped nf-spawn's `checkout` pin to a **v7.0.1** SHA while leaving the
  comment reading `# v6`, and the identical regex passed it. Two complementary
  halves now, because neither alone suffices: the test requires an exact `vX.Y.Z`
  (offline, hermetic — catches vague labels), and a new `scripts/verify-pins.sh`
  resolves each SHA against the tag its comment claims and fails if they disagree
  (needs the network, so it runs as its own CI step — catches exact-but-false
  labels the offline half cannot see). All three `checkout` comments relabelled
  `# v7` → `# v7.0.1`, since the tag `v7` had moved off the pinned commit.

### Security
- **Pinned the 3 remaining floating action tags to commit SHAs, and added
  Dependabot to bump every pin** ([#7](https://github.com/spore-host/airflow-spawn/issues/7)). A tag is mutable — `@v5` means
  "whatever `v5` points at when the job runs" — and `actions/checkout@v6` genuinely
  moved (`df4cb1c` → `d23441a`) with no signal to consumers, so this is not
  hypothetical. It also showed the drift this causes: `ci.yml` and `release.yml` were pinned, but `composition-test.yml` — added later — was not. Pinning and Dependabot are one control, not two: a SHA never
  moves on its own, including past a security fix.
  - The new `.github/dependabot.yml` covers `github-actions` and `pip`, weekly with
    a 7-day cooldown — a freshly published tag is exactly when a compromised or
    broken one is still unnoticed. Group pattern is `*`, not `actions/*`, because
    `softprops/action-gh-release` (which creates the GitHub Release under
    `contents: write`) would otherwise fall outside the group and stop being
    bumped. `ruff >=0.16` is ignored so a bump can't undo the deliberate cap.
  - `tests/test_ci_hygiene.py` makes both halves regressions rather than
    conventions: reverting a pin or dropping the Dependabot entry now fails
    `pytest`, which CI already runs. `pyyaml` joins the `[dev]` extra for it and is
    imported unguarded — a `try`/`except` import degrades to a skip, and a skipped
    wiring test reports green while asserting nothing.
  No behaviour change — CI wiring and tests only.

### Fixed
- **CI is green again, and no longer silently redefines what it enforces.** Two
  failures were latent on `main` — the second hidden behind the first, since the
  lint step aborts the job before the tests run:
  - `ruff check .` went red with no change on our side. ruff 0.16 moved a large
    set of opinionated rules (`BLE`, `PLW`, `TRY`, `C408`, `EXE`, `B017`, …) into
    its **default** rule set, and `ruff>=0.5` was unpinned, so CI adopted 26 new
    violations the moment ruff published. `ruff` is now capped `<0.16` (matching
    `snakemake-executor-plugin-spawn`); adopting those rules should be a
    deliberate change via an explicit `select`, not something a release does to us.
  - `pytest -q` failed at **collection**, not on a test. The engine-composition
    script is named `composition_test.py`, which matches pytest's default
    `*_test.py` discovery, so a bare `pytest -q` imported it and died on
    `os.environ["AWS_ENDPOINT_URL"]` — and a collection error fails the entire run
    rather than skipping one test. Discovery is now scoped to the unit tests. The
    composition workflow is unaffected: it invokes the script directly with
    `python tests/composition/composition_test.py`.

### Changed
- Modernized type annotations in `operator.py`, `taskspec.py` and `trigger.py` to
  the idioms ruff's `UP` rules prefer: `X | None` instead of `Optional[X]`,
  `Sequence`/`AsyncIterator` imported from `collections.abc` instead of `typing`,
  and an unnecessary string annotation unquoted. Behaviour is unchanged: `X | None`
  and the `collections.abc` generics both evaluate at runtime on this package's
  floor (3.10+), so they stay safe for the annotations Airflow introspects
  regardless of `from __future__ import annotations`.

### Added
- **Engine-composition CI test** (`tests/composition/`, `.github/workflows/composition-test.yml`):
  runs the **real `SpawnRunTaskOperator.execute()`** against the
  [Substrate](https://github.com/scttfrdmn/substrate) AWS emulator — no real AWS,
  no cost. Exercises the full seam (operator → real `spawn task run` + real
  `aws s3` staging vs. Substrate → completion record → exit code → return on
  success / `AirflowException` on failure), asserting both the happy path
  (unseeded ⇒ nominal success) and the failure path (a seeded nonzero completion,
  via substrate#360's `POST /v1/spawn/task-completion`, ⇒ the operator raises).

## [0.3.0] - 2026-07-25

### Changed
- **Renamed the package and repo `spawn-airflow` → `airflow-spawn`** for
  consistency with the other workflow adapters (`nf-spawn`, `miniwdl-spawn`,
  `cwl-spawn`, `snakemake-executor-plugin-spawn`), which all follow the
  `<engine>-spawn` convention. The import module is now `airflow_spawn`
  (was `spawn_airflow`) and the PyPI distribution is `airflow-spawn`
  (was `spawn-airflow`). **Breaking** (pre-1.0 → MINOR): update
  `pip install airflow-spawn` and `from airflow_spawn import SpawnRunTaskOperator`.
  Done before any release adoption, so no migration path is provided.

## [0.2.0] - 2026-07-19

### Changed
- **airflow-spawn now dispatches each task through `spawn task run`** instead of
  orchestrating the launch itself (spawn#386 adapter migration). `execute` builds
  a spawn **TaskSpec** and runs `spawn task run` (detached), then polls
  `spawn task status --check-complete` (sync) or defers to `SpawnTaskStatusTrigger`
  (deferrable), reading the exit code from the **CompletionRecord**. spawn now owns
  instance sizing (truffle), S3 staging, the durable completion record, and a
  **scoped least-privilege IAM profile** (was `--iam-policy s3:FullAccess`) — so
  airflow-spawn no longer reimplements any of it.
- **`SpawnExitCodeTrigger` is renamed `SpawnTaskStatusTrigger`** and now polls
  `spawn task status` instead of the raw `.exitcode` S3 object. Its serialized
  kwargs changed from `{s3_prefix, region, poll_interval}` to
  `{task_id, region, poll_interval}`.
- **The `instance_type` argument now steers the instance _family_** (e.g.
  `c7i.4xlarge` → the `c7i` family) rather than pinning the exact type; spawn's
  sizer picks the cheapest fit within it. (Exact-pin support is tracked as a spawn
  TaskSpec follow-up.)
- **The on-instance job dir moved to `/var/tmp/airflow_spawn_job`** (was
  `/mnt/airflow_spawn_job`): spawn runs the command as the instance's unprivileged
  login user, which can't create dirs under the root-owned `/mnt`.
- `truffle` is no longer required on `PATH` (spawn sizes the instance itself);
  `spawn` and AWS credentials are still required.

### Removed
- Bundled launch/completion/bootstrap/sizing machinery (`launch.py`,
  `completion.py`, `bootstrap.py`, `sizing.py`) — spawn owns these now.

## [0.1.0] - 2026-07-07

### Added
- Initial release: `SpawnRunTaskOperator` — an Apache Airflow operator that runs a
  task's command on an ephemeral EC2 instance via spore-host/spawn, auto-sized
  from `cpus`/`memory_gib` via truffle, launched with a TTL and `--on-complete
  terminate`, with a durable `.exitcode`-in-S3 completion signal. Deferrable (frees
  the worker slot while the instance runs) via `SpawnExitCodeTrigger`. The Airflow
  sibling of nf-spawn, miniwdl-spawn, cwl-spawn, and snakemake-executor-plugin-spawn.
- Verified end-to-end on real AWS: a `SpawnRunTaskOperator` task ran its command on
  a spawned EC2 instance (output captured to S3), surfaced exit 0, and the instance
  self-terminated (leak-checked clean).

[Unreleased]: https://github.com/spore-host/airflow-spawn/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/spore-host/airflow-spawn/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/spore-host/airflow-spawn/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/spore-host/airflow-spawn/releases/tag/v0.1.0
