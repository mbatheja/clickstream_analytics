"""
Telemetry Event Generator

The Goal is to simulate user behaviour across e-commerce funnel.
We will use the simulation for A/B experiment and seed downstream
statistical testing pipelines.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import os
import random
import uuid
import yaml

# Import sample size calculator from utils
from src.utils.sample_size import calculate_sample_size

MAX_WORKERS = 20

# Repeat-visit frequency distribution, shared by the local generator and the
# S3 streamer so both simulate the same expected sessions-per-user.
SESSION_COUNT_CHOICES = [1, 2, 3, 4, 5]
SESSION_COUNT_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]


def expected_sessions_per_user() -> float:
    return sum(n * w for n, w in zip(SESSION_COUNT_CHOICES, SESSION_COUNT_WEIGHTS))


def expected_checkout_reach_prob(config: dict, checkout_step: str = "checkout_initiated") -> float:
    """
    Probability that a single session makes it all the way to checkout_step,
    derived from the funnel's own transition probabilities -- the configured
    control/variant conversion rates only apply to sessions that reach this
    stage, so this tells us what fraction of simulated sessions are actually
    "at risk" for the effect being tested.
    """
    funnel_steps = config["funnel"]["steps"]
    base_probs = config["funnel"]["transition_probabilities"]

    prob = 1.0
    for step in funnel_steps:
        if step == checkout_step:
            break
        prob *= base_probs.get(step, 1.0)
    return prob


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Loads external parameters from YAML configuration.
    Edit YAML to change probabilities and funnel stages.
    """
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    full_path = os.path.join(base_dir, config_path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Configuration file not found at: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def simulate_user_session(
    user_id: str,
    config: dict,
    variant: str = None,
    device: str = None,
    channel: str = None,
    signup_date: str = None,
    session_start_time: datetime = None
) -> tuple[list[dict], datetime]:
    """
    Simulates a single user journey through the conversion funnel.
    Accepts optional persistent user attributes to maintain multi-session consistency.
    """
    session_id = str(uuid.uuid4())

    # Assign or retain persistent user attributes
    variant = variant or random.choice(["control", "new_checkout_ui"])
    device = device or random.choice(["mobile", "desktop", "tablet"])
    channel = channel or random.choice(["Organic", "Google_Ads", "Email", "Direct"])
    experiment_id = "exp_checkout_redesign_2026"
    signup_date = signup_date or "2026-06-01"

    # Initialize session start time
    current_time = session_start_time or (datetime.now() - timedelta(minutes=random.randint(1, 300)))

    # Unpack config variables
    funnel_steps = config["funnel"]["steps"]
    base_probs = config["funnel"]["transition_probabilities"]
    exp_config = config["experiments"]["checkout_conversion"]

    events = []

    for i, step in enumerate(funnel_steps):
        # Start at view homepage
        if i == 0:
            should_continue = True

        # Evaluate A/B test transition
        elif funnel_steps[i-1] in ["checkout_initiated", "checkout_initated"]:
            prob = exp_config["variant"] if variant == "new_checkout_ui" else exp_config["control"]
            should_continue = random.random() < prob

        # Evaluate baseline transition probabilities for standard funnel steps
        else:
            prob = base_probs.get(funnel_steps[i-1], 0.0)
            should_continue = random.random() < prob

        # If user fails probability check, they exit
        if not should_continue:
            break

        # Simulate realistic time delay between clicks (15 to 120 sec)
        current_time += timedelta(seconds=random.randint(15, 120))

        # Build the structured telemetry payload
        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": step,
            "timestamp": current_time.isoformat(),
            "device": device,
            "experiment_id": experiment_id,  # Top-level field for easy dbt filtering
            "customer_metadata": {
                "acquisition_channel": channel,
                "signup_date": signup_date,
                "experiment_variant": variant   
            }
        }
        events.append(payload)

    return events, current_time


def run_generator(num_users_override: int = None):
    """
    Generates and persists mock clickstream events using statistical sample size.
    Simulates persistent user cohorts with multi-session activity across several days.

    num_users_override: skip the power analysis and generate exactly this many users
    instead (for quick/test runs).
    """
    config = load_config()
    output_dir = config["simulation"]["output_dir"]
    exp_config = config["experiments"]["checkout_conversion"]

    # Fetch baseline & variant conversion rates from config
    p_control = exp_config["control"]      # e.g., 0.05
    p_variant = exp_config["variant"]      # e.g., 0.055
    relative_mde = (p_variant - p_control) / p_control

    if num_users_override is not None:
        num_users = num_users_override
        print(f"Using overridden user count: {num_users:,} (skipping power analysis)")
    else:
        # Required checkout-reaching sessions per arm (80% Power, 95% Confidence)
        n_per_arm = calculate_sample_size(p1=p_control, relative_mde=relative_mde)
        n_per_arm = n_per_arm[0] if isinstance(n_per_arm, tuple) else n_per_arm

        print(f"Control Conversion Rate:    {p_control * 100:.2f}%")
        print(f"Variant Conversion Rate:    {p_variant * 100:.2f}% (Lift: +{relative_mde * 100:.1f}%)")
        print(f"Required checkout-reaching sessions per arm: {int(n_per_arm):,}")

        # The configured lift only applies to sessions that reach checkout, not
        # every simulated user -- inflate the population so the checkout-reaching
        # subset (not the raw user count) actually hits n_per_arm, per arm.
        checkout_reach_prob = expected_checkout_reach_prob(config)
        sessions_per_user = expected_sessions_per_user()
        print(f"Expected checkout-reach rate per session: {checkout_reach_prob * 100:.2f}%")
        print(f"Expected sessions per user: {sessions_per_user:.2f}")

        total_checkout_sessions_needed = n_per_arm * 2  # both arms
        num_users = total_checkout_sessions_needed / (sessions_per_user * checkout_reach_prob)
        print(f"Inflated required N: {int(num_users):,} total users "
              f"(vs. {int(total_checkout_sessions_needed):,} if every session reached checkout)")

    os.makedirs(output_dir, exist_ok=True)

    start_sim_base = datetime(2026, 6, 1, 9, 0, 0)
    print(f"Generating multi-session clickstream data for {num_users:,} persistent users "
          f"using {MAX_WORKERS} workers...")

    total_events = 0
    total_sessions = 0
    completed_users = 0
    progress_step = max(1, int(num_users) // 100)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(_simulate_and_write_user, idx, config, output_dir, start_sim_base)
            for idx in range(int(num_users))
        ]

        for future in as_completed(futures):
            events_written, sessions_written = future.result()
            total_events += events_written
            total_sessions += sessions_written
            completed_users += 1

            if completed_users % progress_step == 0 or completed_users == int(num_users):
                pct = completed_users / num_users * 100
                print(f"  ...{completed_users:,}/{int(num_users):,} users ({pct:.0f}%), "
                      f"{total_events:,} events written so far")

    print(f"Simulation complete. Generated {total_events:,} raw events across {total_sessions:,} sessions in '{output_dir}/'.")


def _simulate_and_write_user(idx: int, config: dict, output_dir: str, start_sim_base: datetime) -> tuple[int, int]:
    """Simulates one persistent user's full multi-session history and writes each event to disk."""
    user_id = f"usr_{1000 + idx}"

    # Establish immutable attributes per user
    variant = random.choice(["control", "new_checkout_ui"])
    device = random.choice(["mobile", "desktop", "tablet"])
    channel = random.choice(["Organic", "Google_Ads", "Email", "Direct"])

    # User signs up on a specific day in a 14-day window
    signup_dt = start_sim_base + timedelta(days=random.randint(0, 14), hours=random.randint(0, 12))
    signup_date = signup_dt.strftime("%Y-%m-%d")

    # Determine repeat visit frequency (1 to 5 sessions per user)
    num_sessions = random.choices(SESSION_COUNT_CHOICES, weights=SESSION_COUNT_WEIGHTS)[0]
    current_session_time = signup_dt

    events_written = 0
    sessions_written = 0

    for s_idx in range(num_sessions):
        session_events, session_end_time = simulate_user_session(
            user_id=user_id,
            config=config,
            variant=variant,
            device=device,
            channel=channel,
            signup_date=signup_date,
            session_start_time=current_session_time
        )

        # Save each event as an individual JSON file
        for event in session_events:
            file_path = os.path.join(output_dir, f"event_{event['event_id']}.json")
            with open(file_path, "w") as f:
                json.dump(event, f, indent=2)
            events_written += 1

        sessions_written += 1

        # Advance time by 1 to 7 days for subsequent return sessions
        days_until_next = random.randint(1, 7)
        hours_offset = random.randint(1, 8)
        current_session_time = session_end_time + timedelta(days=days_until_next, hours=hours_offset)

    return events_written, sessions_written


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-users", type=int, default=None,
        help="Override the statistically-derived user count (for quick/test runs)"
    )
    args = parser.parse_args()
    run_generator(num_users_override=args.num_users)