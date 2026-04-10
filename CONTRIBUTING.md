# Contributing

Thanks for contributing to Glitch Cricket Engine.

This repo is a live cricket analysis engine with replay, recording, and simulation infrastructure. Please keep changes focused, observable, and safe for production-style review.

## Good Contribution Areas

- innings and chase analysis improvements
- recorder and replay tooling
- provider client hardening
- diagnostics and review workflows
- documentation and operator usability
- tests and smoke checks

## Be Careful With

- `spotter.py` live scan flow
- provider integrations and rate limits
- persistence/state transitions
- anything that changes signal behavior silently

## Local Checks

At minimum, run compile checks for touched modules:

```bash
python3 -m py_compile modules/shared_core.py modules/match_recorder.py modules/paper_simulator.py modules/risk_manager.py
```

If your change touches live signal or paper simulation flow, run a small smoke check too.

## Config and Secrets

Do not commit:
- `ipl_spotter_config.json`
- provider tokens
- local databases
- logs
- model artifacts or temporary data files

Use example/template files when config changes are needed.

## Pull Requests

Please include:
- what changed
- why it changed
- validation you ran
- runtime risk or match scenarios to watch

## Attribution

Please keep the project's license, notice, and authorship files intact.
