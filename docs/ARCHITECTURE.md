# Architecture

## High-Level Flow

1. External providers deliver live match, fixture, squad, and venue context.
2. `MatchState` converts provider payloads into a consistent internal state.
3. `InningsState` derives batting depth, tail strength, remaining batting quality, and bowling resources.
4. Scenario and chase modules estimate how the next phase of the innings can branch.
5. `Predictor` produces session, innings, and match-winner views.
6. `match_context` rules suppress low-quality or contradictory outputs.
7. Recorder, Telegram, and review systems preserve the analysis trail.

## Runtime Layers

### Ingestion
- Sportmonks and adjacent provider clients fetch fixtures, live score, squads, and metadata.
- Weather and auxiliary context enrich pre-match or mid-match interpretation.

### State Normalization
- `modules/match_state.py`
- `modules/innings_state.py`

This layer exists so downstream logic works from a stable internal representation instead of raw provider payloads.

### Projection
- `modules/predictor.py`
- `modules/scenario_model.py`
- `modules/wicket_hazard.py`
- `modules/chase_state.py`

This is where the engine moves beyond plain scoreboard projection toward scenario-aware reasoning.

### Context and Consistency
- `modules/match_context.py`
- `modules/player_context.py`
- `modules/match_dossier.py`

These modules provide vetoes, contradiction checks, and context enrichments that stop low-quality analysis from leaking into signals.

### Output and Review
- `modules/telegram_bot.py`
- `modules/match_recorder.py`
- `modules/paper_simulator.py`
- `modules/shadow_tracker.py`

Even when live execution is not the goal, the review and recording infrastructure is useful because it creates a feedback loop around model behavior.

## Design Direction

The project is moving from an execution-first bot toward an analysis-first engine.

That means:
- more emphasis on explainable live state
- more emphasis on reviewability and signal quality
- less emphasis on blind automation
- better preservation of project knowledge in docs and recorded traces
