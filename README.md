# Glitch Cricket Engine

Live cricket analysis and signal engine for IPL and PSL matches.

This repository contains the production code for a cricket match intelligence bot that:
- ingests live match state from Sportmonks
- builds session and innings projections
- enriches live state with player, squad, venue, and chase context
- generates Telegram-ready signals and analysis
- records match state, signals, and paper-trade outcomes for review

The codebase still contains Cloudbet and paper-trading infrastructure from the live-betting version of the bot, but the current direction is to evolve it into a stronger analysis engine rather than a pure execution bot.

## Current Capabilities

- Live match state normalization via MatchState
- Session market projections for 6_over, 10_over, 15_over, 20_over, and innings_total
- Match winner estimation
- Resource-aware innings modeling with InningsState
- Scenario-tree projections with wicket branching
- Chase pressure classification with ChaseStateMachine
- Match context veto / consistency rules
- Telegram signal formatting and delivery
- Paper simulation and shadow tracking
- Fixture lifecycle support for IPL and PSL

## Repository Layout

- `spotter.py`: main live scan loop and signal pipeline
- `liveline_bot.py`: live line listener
- `modules/`: prediction, context, execution, and integrations
- `series/`: competition-specific profiles and registry
- `tests/`: unit and integration-oriented tests
- `scripts/`: data-building and reporting helpers
- `systemd/`: service files used on the server
- `ipl_spotter_config.example.json`: sanitized example config

## Quick Start

### 1. Clone the repo

```bash
git clone git@github.com:glitch-executor/glitch-cricket-engine.git
cd glitch-cricket-engine
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

### 3. Copy the example config

```bash
cp ipl_spotter_config.example.json ipl_spotter_config.json
```

Fill in your own API keys and runtime settings in `ipl_spotter_config.json`.

### 4. Run the main bot

```bash
python spotter.py
```

### 5. Run the live line listener

```bash
python liveline_bot.py
```

## Configuration

The bot reads runtime settings from `ipl_spotter_config.json`.

Important keys include:
- `sportmonks_api_key`
- `cloudbet_api_key`
- `telegram_bot_token`
- `telegram_chat_id`
- `cricdata_api_key`
- `weather_api_key`
- `brave_api_key`
- `openai_api_key`
- `competitions`
- signal and autobet thresholds

A sanitized example config is included in `ipl_spotter_config.example.json`.

## Services

The repo includes systemd units used on the server:
- `systemd/ipl-bot.service`
- `systemd/ipl-liveline.service`

These are configured to run from `/home/support/workspace/ipl_bot` with:
- `venv/bin/python spotter.py`
- `venv/bin/python liveline_bot.py`

Adjust paths if you deploy somewhere else.

## Security Notes

This repository is intentionally published without live secrets, runtime databases, logs, or the local virtualenv.

Ignored from git:
- `ipl_spotter_config.json`
- `data/`
- `logs/`
- `venv/`
- `.env*`

If you previously kept production keys in local config, rotate them before using this repo in a broader team or public workflow.

## Known Setup Caveats

The current repository was developed on a live server environment, so fresh installs may need a few additional runtime packages beyond the minimal `requirements.txt`, especially for optional ML features.

If you intend to use the ML and scenario stack, verify that your environment includes packages used by the codebase such as:
- `numpy`
- `xgboost`

## Status

This repo is an active working codebase, not a polished framework release. The strongest parts today are:
- live state ingestion
- prediction pipeline wiring
- session and chase analysis flow

The main areas still evolving are:
- deeper scenario modeling
- ML feature alignment
- setup reproducibility
- public-facing documentation
