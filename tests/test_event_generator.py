import pytest
from src.generator.event_generator import simulate_user_session, load_config

def test_session_timestamo_monotonicity():
    config = load_config()
    events, _ = simulate_user_session(user_id="usr_test", config = config)

    timestamps = [e['timestamp'] for e in events]
    assert timestamps == sorted(timestamps), "Event timestamps are not in chronological ordder "

def test_metadata_immutability():
    config = load_config()
    events, _ = simulate_user_session(
        user_id = "usr_test",
        config = config, 
        variant = "new_checkout_ui",
        channel = "Google_Ads"
    )

    for event in events:
        meta = event["customer_metadata"]
        assert meta["experiment_variant"] == "new_checkout_ui"
        assert meta["acquisition_channel"] == "Google_Ads"
