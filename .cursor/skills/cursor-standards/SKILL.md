---
name: cursor-standards
description: >
  Enforces minimal focused code changes, read-before-write exploration, test coverage,
  verify-before-done checks (lint, typecheck, build), and accurate dependency versioning.
  Apply automatically on every coding task — implementation, bug fixes, refactors, and reviews —
  unless the user explicitly requests a broader change.
---

# Cursor Standards

Persistent standards for every Cursor interaction. More principles will be added over time.

## Persistence

**Active on every response** that involves code. Do not drift back to verbose or over-scoped edits as the conversation grows.

---

## 1. Minimal scope

Use the simplest correct diff. Do not add or change unrelated or unrequested code.

### Do

- Solve only what was asked
- Prefer a focused 5-line fix over a 100-line diff
- Match existing conventions — naming, types, imports, abstractions, documentation level
- Reuse and extend existing functions/components rather than reimplementing

### Do not

- Edit code for question-only or review-only tasks unless the user asks
- Add comments, helpers, refactors, or "while I'm here" improvements unless requested or clearly necessary
  (tests are required when adding or changing behavior — see principle 2)
- Over-engineer — no one-off abstractions, excessive error handling for unlikely edges, or speculative features

### Before editing, check

1. Does this change directly solve what was asked?
2. Can it be smaller and still correct?
3. Am I touching files or logic outside the request?

If any answer is no, narrow the change.

---

## 2. Test coverage and passing tests

Any code you add or change must be covered by tests, and those tests must pass before you consider the work done.

### Do

- Add or update tests for every new or changed behavior — logic, APIs, components, utilities, edge cases
- Follow the project's existing test framework, layout, and naming conventions
- Run the relevant test suite after changes and fix failures before finishing
- When fixing a bug, add a regression test that would have caught it

### Do not

- Ship behavior changes with no test updates
- Leave failing or skipped tests behind
- Add tests for unrelated code outside the current change (still follow principle 1)

### Before finishing, check

1. Does every new or changed code path have a test?
2. Did I run the tests and confirm they pass?
3. If tests don't exist yet for this area, did I create them using project patterns?

If any answer is no, add or fix tests before stopping.

---

## 3. Verify before done

After code and test changes, run the project's quality checks and fix failures before considering the work complete.

### Do

- Run the relevant checks for what you changed — tests (principle 2), lint, typecheck, and build when the project provides them
- Use existing scripts from `package.json`, `Makefile`, CI config, or project docs — do not invent commands
- Fix issues introduced by your changes; do not leave broken lint, type errors, or build failures behind
- Scope runs to what changed when possible (e.g. affected test files), but run full lint/build if the project has no narrower target

### Do not

- Stop after editing code without running checks
- Assume tests alone are enough when lint or build would catch import, type, or config errors
- Ignore failures unrelated to your change without at least noting them — fix yours first

### Before finishing, check

1. Did I run tests and confirm they pass? (principle 2)
2. Did I run lint and/or typecheck for the areas I changed?
3. Does the project build successfully (or the equivalent check for this stack)?

If any answer is no, run the check and fix failures before stopping.

---

## 4. Read before write

Understand existing code and conventions before making changes. Do not edit files you have not read.

### Do

- Read the file(s) you plan to change and their immediate callers or consumers
- Check related tests, types, and config before implementing
- Search for existing helpers, components, or patterns that already solve part of the problem
- Follow naming, structure, imports, and abstractions already used in that area of the codebase

### Do not

- Guess at file contents, APIs, or project layout
- Introduce a new pattern when an existing one already fits
- Duplicate logic because you skipped reading nearby code

### Before editing, check

1. Have I read the files I am about to change?
2. Do I know how this area is tested and used?
3. Is there existing code I should extend instead of replacing?

If any answer is no, read or search first — then edit.

---

## 5. Dependency versions

When adding or updating a package, look up the current stable version. Do not guess from memory or use lazy wide ranges.

### Do

- Prefer an existing project dependency over adding a new package (principle 1)
- Before adding or bumping a dependency, look up the latest stable release (web search, PyPI, npm registry, etc.)
- Match the project's pinning style (`==`, `>=`, major-bound ranges) but base constraints on the version you actually looked up
- Pin to a specific version or a tight, intentional range — e.g. if latest is `4.9.0`, use that fact; do not fall back to `>=3,<5`
- When upgrading, replace old constraints — remove obsolete entries for prior major versions unless multi-version support is explicitly required

### Do not

- Add packages without checking whether the project already covers the need
- Use vague catch-all ranges (`>=3,<5`) when you know the target release
- Keep dead backward-compatibility pins (e.g. a v3 line alongside v4) unless the user or project truly needs both
- Rely on outdated training-data version knowledge — verify current releases

### Before adding or changing a dependency, check

1. Can existing dependencies handle this?
2. What is the latest stable version right now (looked up, not guessed)?
3. Does the constraint reflect that version, with no useless legacy pins left behind?

If any answer is no, look it up and fix the constraint before finishing.
