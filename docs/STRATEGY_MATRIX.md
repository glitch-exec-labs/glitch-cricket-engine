# Strategy Matrix

## Analysis Layers by Use Case

| Use Case | Primary Modules | Output Style |
|---|---|---|
| Live session analysis | `match_state`, `innings_state`, `predictor` | session projections and commentary |
| Chase interpretation | `chase_state`, `match_context`, `predictor` | chase pressure and win-tilt views |
| Scenario-driven review | `scenario_model`, `wicket_hazard`, `match_recorder` | expected range and branch-aware insight |
| Telegram publishing | `spotter.py`, `telegram_bot.py` | concise analysis messages |
| Paper review / feedback loop | `paper_simulator`, `shadow_tracker`, recorder modules | post-match evaluation |

## Design Principle

The project is most valuable when these layers work together:
- state first
- scenario second
- output last

That ordering keeps the repo useful as a public engineering project rather than a thin wrapper around provider APIs.
