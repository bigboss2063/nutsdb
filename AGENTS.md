# Repository Guidelines

## Project Structure & Module Organization
- Top-level `.go` files implement the public `nutsdb` package.
- `internal/` contains internal-only components (not part of the public API).
- `docs/` holds user guides and `docs/img` architecture diagrams.
- `examples/` provides runnable usage samples.
- `.github/workflows/` defines CI for build, tests, lint, and typo checks.

## Build, Test, and Development Commands
- `go build ./...`: compile all packages.
- `go test ./...`: run unit tests.
- `go test -race -timeout 15m ./...`: run tests with the race detector (CI default).
- `go test -race -coverprofile=coverage.out -covermode=atomic ./...`: collect coverage.
- `rm -rf /tmp/nutsdb* /tmp/test-hintfile*`: clean test artifacts if tests leave files behind.
Go 1.24+ is required (see `go.mod`).

## Coding Style & Naming Conventions
- Follow Go CodeReviewComments and standard formatting.
- Use `gofmt` (and `gofumpt` where available) before commits.
- Exported identifiers use `CamelCase`; unexported use `lowerCamel`.
- Tests live next to code and use `*_test.go` filenames.

## Testing Guidelines
- Primary framework is Go `testing` with `testify` assertions.
- Prefer table-driven tests and `t.Run` for subcases.
- Add or update tests whenever behavior changes or bugs are fixed.

## Commit & Pull Request Guidelines
- Commit messages commonly follow `type: short summary` (examples: `fix:`, `feat:`, `refactor:`, `chore:`), sometimes with a scope or PR number.
- Follow GitHub Flow: branch from `master`, open a PR, and keep it focused.
- PRs should include a clear description, linked issues (if any), and doc updates for API changes.
- Ensure tests pass and lint is clean (`golangci-lint`, `gofumpt`, `typos` run in CI).
