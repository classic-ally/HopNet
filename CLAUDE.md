# Orchestrator Infrastructure

HopNet includes a Docker-based orchestrator for testing mesh networks. The following Claude infrastructure is available:

**Agents:**
- `orchestrator-runner` (haiku) - Executes orchestrator commands (create mesh, run tests, check divergence). Returns structured results with SUCCESS, FAILURE, or NEEDS_DEBUG status.
- `orchestrator-debugger` (sonnet) - Investigates failures. Analyzes container logs, consensus history, and divergence patterns to identify root causes.

**Skill:**
- `orchestrator-reference` - Command syntax, available tests, and workflow documentation. Loaded automatically by both agents.

**Workflow:**
- When invoking `orchestrator-runner`, include mesh context: which mesh to use (if any exists), whether to reuse or create fresh, and any state requirements.
- When `orchestrator-runner` returns `NEEDS_DEBUG`, automatically invoke `orchestrator-debugger` with the failure context to investigate.

# Documentation Management
- ALWAYS check docs/system-overview.md for current project status and priorities
- When making code changes that affect system status, update progress indicators in:
  - docs/system-overview.md (high-level system component status)
  - Relevant RFCs in docs/specs/ (detailed implementation phase status)
  - Progress indicator format: [x] = Complete, [~] = In Progress, [ ] = Not Started, [!] = Blocked
- When implementing new features, update the relevant system status and progress tracking
- When creating new major subsystems, consider if they need their own RFC in docs/specs/
- Keep docs/system-overview.md as the single source of truth for project status

# Code-Documentation Sync Requirements
When making changes to the codebase:
1. Update progress indicators in docs/system-overview.md to reflect actual implementation status
2. Update corresponding RFC implementation phase status (e.g., Phase 1 [~] to Phase 1 [x] when complete)
3. If adding new major features, update the system component descriptions
4. If changing system architecture, update both the overview and relevant RFCs
5. Ensure the "Current Focus" section reflects what's actually being worked on

# Logging
- Use tracing for all Rust logging
- Use INFO levels sparingly to avoid slowdown due to excessive logging

# Debugging
- macOS debugging with the `log` command requires the use of sudo
- tail the last ~10 lines with cargo check to ensure you can see the final result; only if a build fails do you need to get more output.

# Git Commits
- Never include your attribution in commits

# Important Instruction Reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
When working on HopNet features, proactively maintain documentation sync with code changes.