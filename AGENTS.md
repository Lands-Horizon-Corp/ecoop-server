# Repository Guidance

- Read [CLAUDE.md](CLAUDE.md) for project status, architecture, environment setup, and verified commands. Keep this file focused on agent behavior rather than duplicating that reference.
- This is an early Go scaffold. Check the implementation before assuming a `*.sample.go` file or a method stub is functional; many planned `src/` layers are not wired yet.
- Infrastructure implementations live in `pkg/services/`. Prefer the adjacent `*.types.go` interface as the consumer-facing contract, following the existing service packages.
- Regression tests live in `test/regressions/`, not beside implementation files. Run the narrow relevant test first, then `go test ./...` and `go vet ./...` for broader validation. A passing `go test ./...` currently verifies compilation more than application behavior.
- The Compose stack is for observability, not application dependencies. Configuration is read from environment variables; do not assume a dotenv loader is present.
- Consult the existing `.agents/skills/` guidance when a task matches a Go, SQL, security, or concurrency skill. Avoid changing generated or unrelated files while implementing a focused task.