# Trino agent guidance

## VARIANT feature branch

Read [the tracked specification and plan](docs/variant-shredding.md) first.
Local `docs/variant` and `docs/superpowers` are ignored context; historical
handoffs do not supersede the tracked current guide.

- Work on `variant-filter-pushdown` in this normal checkout. Discover companion
  GeoMesa and exporter locations; do not assume a workstation path.
- Phases 1–4 and bounded phase-5 integer/decimal, single-array-step string and
  object-path file pruning are complete. Source-data pilot evidence supports
  native storage for the tested workloads; production acceptance remains open.
- Preserve stock Trino 484, patched `484-shredded-3` artifacts, toolkit marker
  `484` for stock packaging, and separate fork/stock Maven caches. Do not bundle
  `trino-spi` or `trino-main` or deploy core/planner/SPI Java changes.
- Keep exact function claims distinct from conservative GeoTools candidates.
  UNKNOWN retains rows; native hints retain engine residuals. Preserve visibility,
  deletes, metadata safeguards, path identity, borrowed batch lifetimes and fallback.
- Defaults remain unchanged. Sampling/client candidates/filter/projection/native
  hints are opt-in; Iceberg selective rebuilding independently defaults to true.
- Do not publish, push, deploy or rewrite history without a user request. The
  current consolidation is explicitly authorized; no push is requested.
- Keep dated source hashes and evidence immutable. History consolidation changes
  commit identities, not validation results. Do not claim historical/full gates
  were rerun by a documentation-only update.

For other topics not covered here (Web UI build, release process, IDE setup rationale),
see [`.github/DEVELOPMENT.md`](.github/DEVELOPMENT.md).

## Building

Build, test and pre-PR check commands are in the [Building](.github/DEVELOPMENT.md#building)
section of `DEVELOPMENT.md`.

## Code style

Before writing Java code, read the [Code Style](.github/DEVELOPMENT.md#code-style) section of
`DEVELOPMENT.md`. Before adding or changing configuration or session properties, read its
[Configuration and session properties](.github/DEVELOPMENT.md#configuration-and-session-properties)
section.

## Java formatting

Run `mvnd airstyle:format` after Java edits — the `airstyle-maven-plugin` (`io.airlift:airstyle-maven-plugin`)
applies the canonical Airstyle scheme, which is what CI checks. Scope a single file with
`mvnd -pl <module> airstyle:format -Dincludes=**/FileName.java`, and use `airstyle:check` to verify
without rewriting.

## Commits and pull requests

Pull request descriptions use [`.github/pull_request_template.md`](.github/pull_request_template.md).

CI enforces these rules, so check them before pushing:

- Commit messages: the [check-commit-messages policy](https://github.com/airlift/github-actions/tree/main/check-commit-messages#policy),
  run by the `check-commit-messages` job in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- Commit structure: the `check-commits-dispatcher` and `check-commit` jobs in the same file.
- Pull request description: [`.github/workflows/pr-description-check.yml`](.github/workflows/pr-description-check.yml).
