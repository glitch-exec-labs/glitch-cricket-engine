"""PSL (Pakistan Super League) series profile."""

from series.profile import SeriesProfile

PSL_PROFILE = SeriesProfile(
    series_key="psl",
    display_name="Pakistan Super League",
    # PSL pitches are generally higher-scoring; widen model stability gate
    entry_edge=0.06,
    exit_edge=0.025,
    min_edge_balls=4,
    min_edge_balls_powerplay=6,
    reversal_edge_jump=0.14,
    signal_min_edge_runs=3.0,
    autobet_min_edge_runs=5.0,
    signal_min_ev_pct=5.0,
    autobet_min_ev_pct=10.0,
    cooldown_match_winner_s=60,
    cooldown_session_s=120,             # PSL odds are thinner → longer cooldown
    cooldown_innings_total_s=150,
    cooldown_over_runs_s=45,
    signal_direction_cooldown_s=900,
    signal_flip_min_edge=15.0,
    speed_edge_direction_cooldown_s=600,
    speed_edge_flip_override_edge=20.0,
    model_max_shift_per_scan=20.0,      # PSL model is less stable
    fractional_kelly=0.20,              # smaller edge confidence
    max_position_size_usd=15.0,         # less liquidity on PSL
    max_open_bets=8,
    min_odds=1.35,
    max_odds=4.50,
    daily_loss_limit_usd=30.0,
    session_autobet_min_overs_6=2.5,
    session_autobet_min_overs_10=4.5,
    session_autobet_min_overs_15=7.5,
    session_autobet_early_ev=25.0,
    innings_total_min_overs=10.0,
    innings_total_max_overs_inn1=19.0,
    innings_total_max_overs_inn2=17.0,
    alert_on_edge_only=True,            # v2: disable auto-betting, signals + odds only
)
