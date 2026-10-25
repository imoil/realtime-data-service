# Agent Instructions

## Agent skills

### Issue tracker

Issues and specs live in GitHub Issues (via `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical triage roles mapped 1:1 (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` and `docs/adr/` at the root). See `docs/agents/domain.md`.

### Merger reference (Temporary for Spec 005)

Original legacy repository lives at `C:\Users\imoil\repo\formula-evaluator-service`. Research branches and technical evaluation notes live on `research/*` and `docs/research/`.

### Tooling on Windows Sandbox

Gradle execution requires `BypassSandbox: true` due to Windows loopback socket restrictions on daemon IPC. Prefer `write_file` over shell commands for directory creation.
