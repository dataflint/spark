# Turning this folder into the `dataflint/spark-instrumentation` repo

This `instrumentation-repo/` directory is a **self-contained seed** for a new,
standalone repository. It is committed inside the `dataflint/spark` branch
`claude/instrumentation-extraction-2K3Ip` only so the work isn't lost — it is
**not** meant to live here permanently. Move it out into its own GitHub repo:

## 1. Create the GitHub repo

Create an empty repo `dataflint/spark-instrumentation` on GitHub (no README,
no .gitignore — this seed already has them).

## 2. Extract the seed into it

```bash
# from a checkout of dataflint/spark on the extraction branch
cp -r instrumentation-repo /tmp/spark-instrumentation
cd /tmp/spark-instrumentation

git init -b main
git add .
git commit -m "Initial import: SQL-plan instrumentation extracted from dataflint/spark"
git remote add origin git@github.com:dataflint/spark-instrumentation.git
git push -u origin main
```

## 3. Wire up publishing

* The publish workflow (`.github/workflows/publish.yml`) uses the built-in
  `GITHUB_TOKEN` with `packages: write` — no extra secrets needed.
* Confirm `dataflintCommonVersion` in `build.sbt` points at a **released**
  `io.dataflint:dataflint-common` on Maven Central (not a `-SNAPSHOT`). The
  extraction keeps the `INSTRUMENT_*` constants + `registerInstrumentationExtension`
  + `api.ResolvedStageGroup` in `dataflint-common`, so any release at or after
  the extraction point works.
* Tag `v0.1.0` to cut the first release.

## 4. Remove this seed from dataflint/spark

Once the new repo is live, delete `instrumentation-repo/` from the
`dataflint/spark` branch in a follow-up commit — it has served its purpose as a
handoff.

## What was removed from dataflint/spark

See the companion `INSTRUMENTATION-EXTRACTION.md` at the root of the
`dataflint/spark` branch for the exact list of files removed and what was
deliberately kept (the loose-coupling contract).
