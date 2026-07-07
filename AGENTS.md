# AGENTS.md

Repository-level instructions for AI coding agents.

These instructions are intended for GitHub Copilot coding agent first, but should also be useful for other coding agents that read repository instruction files.

Human developers remain responsible for intent, review, merge, deployment, and production outcomes.

---

## Repository Overview

A bundle of custom Apache NiFi processors used in production pipelines for text material at the National Library of Norway (Nasjonalbiblioteket), maintained by Team Tekst. Processors handle tasks such as downloading files from S3 object storage, running JHOVE file-format validation, and generating/validating METS and MIX XML metadata.

This repository contains:

- `nifi-tekst-bundle-processors` - The custom NiFi processors and supporting code (Kotlin)
- `nifi-tekst-bundle-nar` - Packages the Kotlin processors into a deployable NiFi NAR archive
- `nifi-tekst-python-bundle` - Python-based NiFi processors packaged as a NAR archive
- `nifi-docker` - Local Docker setup for running NiFi with the bundle (see `docker-compose.yml`)

Primary technologies:

- Languages: Kotlin (with JVM interop); Python 3.11; some resources in XML/XSD
- Framework: Apache NiFi 2.3.0 extension bundle
- Package managers: Maven (JVM dependencies from Maven Central and NB Artifactory); uv (Python dependencies)
- Runtime: JDK 21 (Temurin, per `.sdkmanrc`); Python 3.11 (inside the NiFi Docker container)
- Test frameworks: JUnit 5 (Jupiter) with `nifi-mock` and Testcontainers (Minio); pytest for Python
- Build system: Maven 3.9 (multi-module reactor; Java NAR via `nifi-nar-maven-plugin`; Python NAR via `maven-assembly-plugin`)

---

## Agent Operating Rules

Before making changes, the agent should:

1. Read the linked Jira issue or GitHub issue carefully.
2. Identify the smallest safe implementation.
3. Prefer existing patterns over introducing new ones.
4. Avoid unrelated refactors.
5. Avoid formatting-only churn.
6. Avoid new dependencies unless explicitly requested or approved.
7. Add or update tests when behavior changes.
8. Run relevant checks before opening a pull request.
9. Explain what changed, how it was verified, and any assumptions made.

The agent must not:

- Change a processor's public contract (relationships, property descriptors, flowfile attributes) unless the issue explicitly requires it.
- Change credentials/secrets handling, the CI workflow, or Artifactory/deployment config unless explicitly assigned by a human.
- Change NiFi or major dependency versions, or alter NAR packaging, without explicit approval.
- Remove, skip, or weaken tests to make CI pass.
- Silence errors without explaining why.
- Add broad try/catch blocks that hide failures.
- Introduce new architectural patterns without approval.
- Make large multi-area changes for a small issue.

---

## Task Suitability

Tasks labeled `agent-ready` may be attempted directly by an agent.

Tasks labeled `agent-assisted` should be driven by a human developer, with the agent helping on investigation, implementation drafts, tests, or refactoring.

Tasks without either label are human-owned by default.

Good `agent-ready` tasks:

- Add or update tests
- Fix lint or type errors
- Update documentation
- Add simple validation logic to a processor
- Fix simple bugs with a known cause
- Perform mechanical refactors
- Update dependency or configuration files with clear scope

Poor `agent-ready` tasks:

- Architecture decisions
- Security-sensitive changes (credentials, secrets, CI/deployment)
- NiFi version upgrades or NAR/packaging changes
- Production incident fixes
- Large refactors
- Ambiguous behavior or unclear acceptance criteria
- Complex performance work
- Changes to how production data in S3/object storage is written or deleted

---

## Jira Issue Expectations

The Jira issue (project `TT`, Team Tekst) is the source of truth for intent. External bug reports may also arrive as GitHub issues, but agent work is driven by the Jira issue.

A good `agent-ready` issue should include:

- Problem statement
- Acceptance criteria
- Relevant area of the codebase
- Expected behavior
- Current behavior, if fixing a bug
- Non-goals
- Verification steps
- Screenshots, logs, examples, or sample files (e.g. METS/MIX XML, file fixtures) if relevant

If the issue is unclear, the agent should avoid guessing. Instead, it should stop and leave a clear comment explaining what needs clarification.

Use the Jira key in branch names and PR titles when possible.

Example:

```text
feat/jhove-validation-TT-2225
```

---

## Branch and Commit Naming

Branches, commits, and PR titles should reference the Jira key (project `TT`) so work is traceable back to its issue.

### Branches

Format: `<type>/<short-kebab-description>-<TT-key>`

```text
feat/jhove-flyt-TT-2225
```

Types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`.

### Commit messages

Format: `<Type>: <imperative summary>` (capitalized type, colon, concise summary).

```text
Fix: generate JHOVE OCR for primary representation when no access exists
```

- Use the body to explain *why* the change was made, not just *what* changed.
- Reference the Jira key in the body or summary when it adds traceability.

### Pull request titles

Format: `<Type>: <Summary> (<TT-key>)`

```text
Feat: Jhove flyt (TT-2225)
```

---

## Code Style

Follow existing code style in nearby files.

General rules:

- Keep changes small and focused.
- Prefer readability over cleverness.
- Use descriptive names.
- Avoid duplicating business logic.
- Prefer explicit types where helpful.
- Keep functions small and testable.
- Follow existing error-handling patterns.
- Do not introduce new abstractions unless they reduce real duplication.

Formatting rules:

- Use the repository's configured formatter.
- Do not reformat unrelated files.
- Do not change line endings, whitespace, or import order outside touched files unless required by tooling.

---

## Project Commands

All commands run from the repository root. Use JDK 21 and Maven 3.9 (see `.sdkmanrc`). Git LFS must be installed and pulled (`git lfs pull`) for the test fixtures to be present.

Resolve dependencies and compile (the Kotlin compiler enforces types; there is no separate type-check step):

```bash
mvn clean compile
```

Linting: there is no separate linter configured. Match the existing Kotlin style in nearby files.

Run tests (unit and Testcontainers-based tests run under Surefire; Docker must be running for the Minio/Testcontainers tests):

```bash
mvn test
```

Run tests for a single module:

```bash
mvn -pl nifi-tekst-bundle-processors test
```

Run all checks and build the deployable NARs before opening a PR (this is what CI verifies):

```bash
mvn clean package
```

Build without tests (e.g. for local Docker iteration):

```bash
mvn clean package -DskipTests
```

Run only the Python tests:

```bash
mvn test -pl nifi-tekst-python-bundle
```

Skip only the Python tests (useful when `uv` is unavailable):

```bash
mvn clean package -DskipPythonTests
```

Rebuild only the Python bundle (faster iteration on Python processors):

```bash
mvn package -pl nifi-tekst-python-bundle -DskipPythonTests
```

For local end-to-end verification in NiFi, see `README.md` (Docker Compose setup at https://localhost:8443/nifi/).

---

## Testing Expectations

When behavior changes, add or update tests.

Preferred test locations:

- Kotlin unit tests: `nifi-tekst-bundle-processors/src/test/kotlin` (mirror the package of the code under test)
- Kotlin processor tests: use `nifi-mock` (`TestRunners`) to exercise processors in isolation
- Kotlin integration tests: same source tree, using Testcontainers (Minio) — these require a running Docker daemon
- Kotlin test fixtures/sample files: `nifi-tekst-bundle-processors/src/test/resources` (tracked with Git LFS)
- Python tests: `nifi-tekst-python-bundle/src/test/python/` — run via `uv run pytest` or `mvn test -pl nifi-tekst-python-bundle`
- End-to-end verification: manual, via the local Docker/NiFi setup described in `README.md`

Testing rules:

- Test behavior, not implementation details.
- Include edge cases from the Jira issue.
- Do not delete, skip, or weaken existing tests.
- If a test cannot be added, explain why in the PR.
- If tests cannot be run locally (e.g. Docker unavailable for Testcontainers), explain why in the PR.

---

## Pull Request Expectations

Agent-created PRs should be small and reviewable.

The PR description should include:

```md
## Summary

- What changed
- Why it changed

## Verification

- [ ] `mvn clean package` passes (compile + tests + NAR build)
- [ ] Testcontainers/integration tests run (or noted why Docker was unavailable)
- [ ] Manual verification in local NiFi (if processor behavior changed)

## Notes for Reviewer

- Risk areas
- Files worth reviewing closely
- Assumptions made
```

The PR should link to the Jira issue.

A human reviewer must approve the PR before merge.

---

## Security and Privacy

The agent must not:

- Print secrets, tokens, credentials, or private keys.
- Add secrets to source code.
- Log sensitive user data.
- Weaken authentication or authorization checks.
- Disable security checks.
- Introduce external network calls unless explicitly required.
- Add telemetry without approval.

Sensitive areas requiring extra human review:

- Credentials for S3/Minio, Artifactory, or the GitHub registry (kept in Vault, never in source)
- The CI workflow (`.github/workflows/`) and its Vault/secret wiring
- Deployment-related config (NAR packaging, `distributionManagement`, Artifactory upload)
- Dependency and version changes affecting the NAR or NiFi compatibility

---

## Dependencies

Do not add new dependencies unless the issue explicitly asks for it or a human approves it.

If a dependency is necessary, explain:

- Why it is needed
- Why existing dependencies are insufficient
- Maintenance status
- Security considerations
- Bundle size or runtime impact, if relevant

---

## Processor Guidelines

### Kotlin processors (`nifi-tekst-bundle-processors`)

Follow existing patterns for NiFi processors in `nifi-tekst-bundle-processors`.

Rules:

- Define relationships, properties, and validators using the standard NiFi APIs, consistent with existing processors.
- Route flowfiles to a `failure` (or equivalent) relationship instead of swallowing errors; preserve existing routing semantics.
- Register new processors in `META-INF/services/org.apache.nifi.processor.Processor`.
- Keep `@CapabilityDescription` accurate (e.g. don't claim "recursive" behavior the code doesn't have).
- Validate inputs at the processor boundary; handle errors consistently with nearby processors.
- When touching XML/METS/MIX handling, keep the bundled XSDs and offline entity resolution working (avoid introducing external HTTP schema lookups).
- Add tests (`nifi-mock` and, where relevant, Testcontainers) for new behavior and edge cases.

### Python processors (`nifi-tekst-python-bundle`)

Follow existing patterns in `nifi-tekst-python-bundle/src/main/python/`.

Rules:

- Declare `PropertyDescriptor` objects as **class-level attributes** (not in `__init__`); NiFi discovers them before instantiation.
- Keep `__init__(self, **kwargs)` calling `super().__init__()` (without `**kwargs`) to absorb the `jvm` argument the NiFi framework injects.
- Use `try/except ImportError` for intra-package imports so they work both as a package (tests) and as flat modules (NiFi runtime).
- Do not add third-party runtime dependencies to `ProcessorDetails.dependencies` — all runtime deps are bundled in the NAR via `NAR-INF/bundled-dependencies/`. To add a new runtime dep, list it explicitly in the `install-python-bundled-deps` exec step in `nifi-tekst-python-bundle/pom.xml` and in the `dependencies` section of `pyproject.toml`. `pyproject.toml` is the single source of truth for both runtime and test deps.
- Add pytest tests in `src/test/python/` for new behavior.

---

## Logging and Observability

Follow existing logging patterns.

Rules:

- Do not log secrets or sensitive user data.
- Keep logs useful and concise.
- Add metrics or traces only if the repository already uses them and the issue requires it.
- Do not introduce noisy logs.

---

## Performance

Do not optimize prematurely.

When changing performance-sensitive code:

- Preserve existing complexity characteristics where possible.
- Avoid unnecessary network calls (e.g. extra S3 round-trips or external schema lookups).
- Be mindful of memory and streaming when processing large files; prefer streaming over loading whole flowfiles into memory where the existing code does.
- Mention performance implications in the PR.

---

## Documentation

Update documentation when behavior, commands, configuration, or public APIs change.

Relevant docs:

- `README.md` (Norwegian) - local Docker development, build/deploy, and maintenance notes
- `docker-compose.yml` - local NiFi setup and volume mounts
- `nifi-tekst-python-bundle/docs/find-crop-approach.md` - algorithm design for the FindCrop processor
- This file (`AGENTS.md`) - keep updated when commands, modules, or conventions change

---

## Failure Handling

If the agent cannot complete the task safely, it should stop and explain why.

Common reasons to stop:

- Requirements are ambiguous.
- Required credentials or services are unavailable.
- Tests cannot be run.
- The change touches high-risk areas not covered by the issue.
- The implementation requires architectural decisions.
- The agent finds conflicting patterns in the codebase.

When stopping, leave a comment with:

```md
## Blocked

Reason:

What I found:

Suggested next step:
```

---

## Review Checklist for Humans

Reviewers should check:

- Does the change actually solve the Jira issue?
- Is the PR smaller than expected?
- Are there unrelated changes?
- Are tests meaningful?
- Did the agent follow existing patterns?
- Are there hidden security or permission implications?
- Are errors handled and routed correctly (no swallowed exceptions)?
- Does the NAR still build (`mvn clean package`) and do tests pass?
