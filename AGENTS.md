# Repository Guidelines

## Project Structure & Module Organization
KanjiCards loads as an Anki add-on through the top-level `__init__.py`, which wires up GUI hooks, sync workflows, and helper utilities. Shared metadata sits alongside it: `manifest.json` defines the add-on package, `config.json` captures default runtime settings, and `kanjidic2.xml` provides the bundled dictionary used in tests. All automated checks live under `tests/`, grouped by feature so new scenarios can slot next to the behaviour they exercise. Keep `shell.nix` aligned with Python tooling updates so contributors can reproduce the same environment.

## Build, Test, and Development Commands
- `nix-shell` — drop into a reproducible dev shell with pytest and coverage preinstalled.
- `pytest` — run the full test matrix with coverage thresholds enforced by `pytest.ini`.
- `pytest tests/test_realtime_and_helpers.py -k import` — target focused investigations without losing fixtures.
- `python -m compileall .` — optional smoke check to catch syntax regressions before shipping.

## Coding Style & Naming Conventions
Match the existing PEP 8 style: four-space indents, 120-character practical ceiling, and generous docstrings for user-facing flows. Prefer `snake_case` for functions, helpers, and module-level constants; reserve `CamelCase` for Qt widgets and dataclasses such as `VocabNoteTypeConfig`. Type hints are expected for new public interfaces, with `Optional` and `Sequence` mirroring how the add-on exchanges data with Anki’s APIs. When touching Qt code, keep translatable strings centralized so localized builds stay manageable.

## Testing Guidelines
Pytest is the single source of truth. Place new unit tests under `tests/` using the `test_<feature>.py` pattern and name individual cases for the scenario they cover (`test_imports_unknown_dictionary`). Every feature PR should refresh or extend the associated fixtures in `conftest.py` rather than creating ad hoc mocks. Aim to preserve current coverage; the default run (`pytest`) already fails if new lines drop below the recorded thresholds.

## Commit & Pull Request Guidelines
Follow the existing history: short, imperative commit subjects (e.g., “Skip post-sync kanji updates when vocab unchanged”) with optional body paragraphs describing rationale and follow-up work. Squash commits that only fix review feedback. Pull requests need: a concise summary of the user-facing change, links to any tracked issues, a note on configuration migrations, and explicit confirmation of the test commands you executed. Include screenshots or GIFs when Qt surfaces shift so reviewers can verify UI implications quickly.
