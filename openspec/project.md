# Project Context

## Purpose
NutsDB is a simple, fast, embeddable and persistent key/value store written in pure Go.

Primary goals:
- Provide an embedded KV store with a small API surface (library, not a service).
- Offer fully-serializable transactions (`Tx`) and multiple data structures (KV, List, Set, Sorted Set).
- Persist data on local disk with predictable startup/recovery and compaction (merge) behavior.
- Keep performance and memory usage suitable for large datasets.

## Tech Stack
- Language: Go module `github.com/nutsdb/nutsdb` (`go 1.24` in `go.mod`; CI uses Go 1.24.x).
- Runtime: local filesystem (MacOS/Linux/Windows); no network services required.
- Core dependencies:
  - `github.com/gofrs/flock` (directory lock to prevent multi-process open)
  - `github.com/edsrzf/mmap-go` (optional mmap RW mode)
  - `github.com/tidwall/btree` (in-memory indexing structures)
  - `github.com/bwmarrin/snowflake` (ID generation)
  - `github.com/stretchr/testify` (tests)
  - `github.com/pkg/errors` (wrapped errors in some paths)
- CI checks (GitHub Actions): build, `go test -race`, coverage upload, `golangci-lint` via reviewdog, `gofumpt -s`, typos.

## Project Conventions

### Code Style
- Follow standard Go style (see Go CodeReviewComments).
- Format: `gofmt`; CI also enforces `gofumpt -s` diffs.
- Prefer clear, explicit names over abbreviations (unless widely standard in Go).
- Public API lives in package `nutsdb`; `internal/` is for non-exported implementation details.
- Error style:
  - Prefer wrapping with context (`fmt.Errorf("...: %w", err)` or `errors.Wrapf` where already used).
  - Exported sentinel errors should remain stable (avoid changing error strings relied upon by callers).

### Architecture Patterns
- Embedded library design: users call `nutsdb.Open(...)` with `Options` and functional `WithX(...)` option helpers.
- Transaction-centric API: all read/write operations occur inside a `Tx` (read-only via `View`, read-write via `Update`).
- Buckets: logical namespaces for keys; buckets are managed on disk and used to separate data structures.
- On-disk storage: append-only data files segmented by `SegmentSize`, plus in-memory indexes rebuilt on startup.
- Indexing modes:
  - `HintKeyValAndRAMIdxMode`: in-memory key+value indexing (higher memory).
  - `HintKeyAndRAMIdxMode`: in-memory key-only indexing (lower memory, value read from disk).
- Background/maintenance features:
  - Merge/compaction pipeline (legacy + Merge V2 when enabled).
  - Optional hint files (when enabled) to speed index reconstruction at startup.
- Cross-platform constraints: filesystem semantics and locking must work on MacOS/Linux/Windows.

### Testing Strategy
- Unit tests and benchmarks live alongside code as `*_test.go`.
- Prefer table-driven tests; use `testify/require` (and `assert` when appropriate).
- CI runs `go test ./...` with `-race` and timeouts; keep tests deterministic and avoid flaky timing assumptions.
- Many tests use temp directories under `/tmp` (e.g., `/tmp/nutsdb*`); ensure tests clean up after themselves.
- Add regression tests for bug fixes and coverage for new behaviors, especially around recovery/merge/index rebuild.

### Git Workflow
- Branching: GitHub Flow; default branch is `master`.
- Changes: small PRs preferred; include tests and documentation updates when changing behavior or public APIs.
- Review gates: CI must pass (build, tests, lint/format checks).

## Domain Context
Key concepts you’ll see throughout the codebase:
- **DB**: database handle bound to an on-disk directory and a file lock (`nutsdb-flock`).
- **Tx**: transaction context for all operations; can be read-only or read-write.
- **Bucket**: a named namespace for keys, partitioned by data structure type.
- **Entry / DataFile / Segment**: persisted records stored append-only; segments roll by size.
- **Merge (compaction)**: rewrites live entries into new files and cleans up obsolete ones.
- **HintFile**: optional persisted index metadata to accelerate startup index rebuild.
- **TTL**: expiration tracking and background scanning to delete/ignore expired keys.

## Important Constraints
- Backward compatibility: data format changes may require migration (notably around v1.0.0 per README); avoid silent format changes.
- Performance-sensitive code paths: avoid unnecessary allocations and extra copying in write/read hot paths.
- Safety: the DB directory is locked for exclusive open; do not introduce behaviors that bypass locking.
- Disk and FD usage: segment size and FD cache settings affect resource usage; keep defaults and documentation consistent.

## External Dependencies
- No external services (embedded library).
- Optional tooling integrations in CI: Codecov upload, reviewdog for lint/format feedback, `typos` action for spelling checks.
