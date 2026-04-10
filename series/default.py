"""Default (conservative) series profile for unknown competitions."""

from series.profile import SeriesProfile

DEFAULT_PROFILE = SeriesProfile(
    series_key="unknown",
    display_name="Unknown Series",
    # Conservative: higher thresholds, longer cooldowns, alerts only
    entry_edge=0.08,
    exit_edge=0.03,
    min_edge_balls=5,
    min_edge_balls_powerplay=7,
    reversal_edge_jump=0.15,
    signal_min_edge_runs=3.0,
    autobet_min_edge_runs=6.0,
    signal_min_ev_pct=8.0,
    autobet_min_ev_pct=15.0,
    cooldown_match_winner_s=90,
    cooldown_session_s=150,
    cooldown_innings_total_s=180,
    cooldown_over_runs_s=60,
    signal_direction_cooldown_s=1200,
    signal_flip_min_edge=20.0,
    speed_edge_direction_cooldown_s=900,
    speed_edge_flip_override_edge=25.0,
    model_max_shift_per_scan=20.0,
    fractional_kelly=0.15,
    max_position_size_usd=5.0,
    max_open_bets=5,
    min_odds=1.40,
    max_odds=4.00,
    daily_loss_limit_usd=15.0,
    alert_on_edge_only=True,           # don't auto-bet on unknown series
)
