# Platform Map

## Primary Inputs

- **Sportmonks**: live cricket fixtures, scores, and detailed match state
- **CricData**: series, squads, scorecards, and supplemental cricket data
- **Weather API**: match-condition enrichment
- **Brave / LLM Intel hooks**: optional narrative or contextual enrichment paths

## Internal Platforms

- **MatchState**: normalized live match state
- **InningsState**: resource model for who and what is left in the innings
- **Scenario Model**: branching projection layer
- **Chase State**: second-innings pressure classification
- **Recorder / Review**: persistence for analysis and paper-review workflows

## Output Surfaces

- Telegram analysis and signal formatting
- local state / review databases in ignored runtime paths
- reporting scripts and diagnostics

## Deployment Surface

- local Python runtime
- systemd services under `systemd/`
- Windows VM notes for alternative deployment environments
