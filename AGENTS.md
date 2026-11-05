# Working on compaction-cli

This is a Go command, not a client SDK. Keep the CLI small and preserve its
saved-plan, state and output contracts.

## Code map

- `main.go`: flags, TOML settings, validation and MySQL connection setup.
- `cli.go`: command lifecycle, output, plan/state persistence and recovery.
- `db.go`: bounded SQL, audit writes and ADMIN outcome classification.
- `engine.go`: planning and serial execution through the consumer-owned `Backend`.
- `skills/compaction-cli/SKILL.md`: embedded operating instructions for agents.
- `docs/usage.md`: command contracts; `docs/goal-mode.md`: ongoing operation.

## Go conventions

Use [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) as the
baseline. [Effective Go](https://go.dev/doc/effective_go) covers language idioms;
it is not a current modules or tooling guide. Google's
[style guide](https://google.github.io/styleguide/go/guide) and
[decisions](https://google.github.io/styleguide/go/decisions) provide further
review guidance. The requirements below are this repository's choices.

- Run gofmt. Use MixedCaps and consistent initialisms (`ID`, `SQL`, `HTTP`).
  Use short names for short scopes; use `options`, `diagnosis`, `window` and
  similarly specific names when values span a function. Keep receiver names
  consistent. Avoid names such as `manager`, `helper` or `util` without a role.
- Document exported types/functions and package purpose. Explain units, ownership,
  side effects and uncertain outcomes where relevant. Comments should add a
  contract or reason, not repeat an assignment. Test names can describe assertions.
- Return errors; use `errors.Is`/`errors.As` and wrap with `%w` when callers need
  the cause. Prefer typed errors for recovery decisions over message matching.
  Preserve exit codes, redaction and unknown-outcome semantics when refactoring.
- Pass `context.Context` first and propagate deadlines. Every goroutine needs a
  defined termination path. Keep credentials out of config, saved state and logs.
  Check write/flush/close failures when they affect persisted data; explain any
  ignored error whose significance is not obvious.

## Design review

Lint checks conventions, not whether an abstraction is useful. Review these
points explicitly when changing control flow or adding an interface:

- Put interfaces at the consumer and include only required operations. Prefer
  concrete return types. Add an interface for a real substitution/test boundary,
  not for every struct. Reuse `Backend` where its contract fits.
- Separate parsing, SQL, execution policy and rendering. Split functions at
  meaningful responsibilities, not just to meet a line or complexity threshold.
  Avoid option bags and mutable shared state spreading across layers.
- Keep state transitions and ownership visible. Establish cleanup in the scope
  that acquires a connection, lock or file. Prefer early returns to deep nesting.
- Use the standard library and existing dependencies when they suffice. Add a
  package or generic abstraction only when it removes demonstrated duplication
  without hiding behavior. Do not introduce a framework for one command.

## Invariants to preserve

- Manifest queries must have a table filter and LIMIT. Planning must retain range
  coverage within its query budget. Capped counts are never exact counts.
- ADMIN submissions are serial. Never retry an uncertain outcome automatically.
  A timeout or stable sample does not establish that background work stopped.
- Persist intent before submitting ADMIN. Hold the state lock throughout execution
  and revalidate loaded state under the lock. Resumes retain the call budget.
- JSON keys and saved schema are compatibility contracts, including fields that
  currently use Go names. Internal renames must not silently change serialization.
- Physical metric storage can be shared; never silently replace a logical table
  with its physical table. Report missing or partial statistics explicitly.

## Validation

Run `make check` and `make lint` before proposing changes. CI pins the lint tool
version; use that version locally. `.golangci.yml` checks names, documentation,
error handling, SQL cleanup, interface size and branching complexity. Current
limits (cyclomatic complexity 20 and 5 interface methods) are project review triggers, not
universal Go rules. Do not suppress a finding just to make CI green; any necessary
`nolint` must name the rule and explain the exception.

Add focused regression tests for changed behavior. Use a real standalone E2E for
SQL, authentication, interruption or recovery changes; unit fakes cannot establish
server behavior. Keep tests about observable contracts. For performance claims,
hold fixture, build profile and resources constant and preserve raw results.
See [validation commands](docs/usage.md#development-ci-and-validation).

Update configuration comments and usage docs with flag/default changes. Keep
README short. Run the license checker when adding source files. Obtain explicit
approval before pushes, PR changes, releases or other remote writes.
