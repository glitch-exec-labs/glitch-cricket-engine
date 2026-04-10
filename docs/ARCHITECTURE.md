# Cricket Engine Architecture

## High-Level Flow

1. External providers deliver live match, fixture, squad, and venue context.
2. `MatchState` converts feed payloads into a consistent internal state.
3. `InningsState` derives resources still available in the match.
4. Scenario and chase modules estimate how the next phase of the innings can branch.
5. `Predictor` produces session, innings, and match-winner views.
6. Context rules suppress low-quality or contradictory outputs.
7. Signals, recorder data, and paper-trade traces are persisted for review.

## Main Runtime Components

### `spotter.py`
Owns the live scan loop, provider orchestration, prediction calls, signal gating, Telegram dispatch, and recording.

### `modules/match_state.py`
Normalizes raw provider data into a stable live state object.

### `modules/innings_state.py`
Derives batting depth, tail strength, remaining batting quality, and bowling resources.

### `modules/scenario_model.py`
Projects forward by branching on likely wicket and run outcomes rather than relying on a single linear pace extrapolation.

### `modules/chase_state.py`
Classifies second-innings chases into pressure bands so signal quality bars can adapt to game state.

### `modules/predictor.py`
Blends baseline heuristics, resource adjustments, and the newer scenario/chase layers into session and innings views.

### `modules/match_context.py`
Contains contradiction rules and qualitative vetoes that stop the engine from emitting low-quality or internally inconsistent takes.

## Design Intent

This engine is being moved from a pure "bet executor" mindset toward an "analysis-first" model. That means:

- more emphasis on explainable state
- more emphasis on model review and paper traces
- less emphasis on blind execution
- tighter feedback loops between live decisions and post-match review

## Current Gaps

- reproducible dependency setup is still thinner than the live server reality
- some ML paths need stronger validation and feature alignment
- matchup-level cricket reasoning can still go deeper

Those gaps are real, but the runtime architecture already supports iterative improvement without a full rewrite.
