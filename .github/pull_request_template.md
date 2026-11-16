## What changed

## Checklist

- [ ] Every new test has a case ID in `tests/cases.json` and a positive / negative marker
- [ ] Expected values come from the source (`SourceReader`), not from the warehouse
- [ ] No assertion was changed just to make a test pass; if an expectation changed, the requirement is linked
- [ ] Anything asserted that the requirement does not state is marked `# ASSUMPTION:`
- [ ] A new check has a fault-injection case proving it fails on its defect
- [ ] Test data is created by fixtures and does not depend on another test
- [ ] `ruff check .`, `mypy src fixtures` and `pytest -n auto` pass locally
