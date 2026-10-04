# Agent rules — dagster-dbt

Lab project: Dagster + dbt (`jaffle_shop`). Python assets in `src/dagster_dbt_demo/defs/`, dbt project in `dbt/` (profiles in `dbt/profiles.yml`, targets: `dev` duckdb, `postgres`, `doris` — default; connection via `DORIS_*` in `.env`, see `.env.example`).

## Rule: log every fixed error

Whenever an error is fixed (or a proactive change is made to prevent one), append a new entry to [`docs/troubleshooting_log.md`](docs/troubleshooting_log.md) **in the same task**, before reporting done.

- Add it as the next numbered section `## N. <error message or short title>`, placed **before** the `## Tổng kết kiến trúc sau khi fix` section, separated by `---`.
- Write in Vietnamese, following the existing entries:
  - **Nguyên nhân:** root cause (what actually broke and why).
  - **Cách fix:** what was changed / commands to run.
  - **Code trước / Code sau (`path/to/file`):** relevant snippets when code changed.
- Proactive changes (not fixing an actual error) are titled `## N. Bổ sung: ...`.
- If the cause is a guess or the fix was not verified end-to-end, say so in the entry.
- Update `## Tổng kết kiến trúc sau khi fix` if the architecture changed.

## Known environment gotchas

- Project lives under `~/Desktop` (iCloud-synced): files can become `dataless` and hang reads. Keep `.venv` and `DAGSTER_HOME` outside iCloud (`DAGSTER_HOME=~/.dagster_home`). See log entries 10–12, 15. Check with `find . -path ./.venv.nosync -prune -o -flags +dataless -print`.
- `dg` shells out to `dbt`: activate `.venv` first, or `dg check defs` fails with "The dbt executable 'dbt' does not exist".
- Fresh clone: `dg check defs` / `pytest` fail with `DagsterDbtManifestNotFoundError: .../dbt/target/manifest.json does not exist` until dbt has produced a manifest. `my_python_script/defs.yaml` reads it via `asset_key_for_model`, and `dbt/target/` is gitignored. Run once: `cd dbt && set -a && source ../.env && set +a && dbt parse --profiles-dir .` (or start `dg dev`).
- After changing the dbt project, clear Dagster's cached copy: `rm -rf src/dagster_dbt_demo/defs/.local_defs_state` (log entry 9).
