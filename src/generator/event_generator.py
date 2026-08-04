"""
Telemetry Event Generator

The Goal is to simulate user behaviour across e-commerce funnel.
We will use the simulation for A/B experiment and seed downstream
statistical testing pipelines.
"""
from datetime import datetime, timedelta
import json
import os
import random
import uuid
import yaml

def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Loads external parameters from YAML configuration.
    Edit YAML to change sample size and probabilities.
    """

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    full_path = os.path.join(base_dir, config_path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Configuration file not found at: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)

def simulate_user_session(user_id: str, config: dict) -> list[dict]:
    """
    Simulates a single user journey through the conversion funnel.

    Flow:
    1. Assign user to an A/B test variant (1:1 randomized split)
    2. Step through funnel stages sequentially
    3. At each stage, generate random number between 0.0 and 1.0.
       If random.random() < step_probability, the user advances, otherwise drop-off
    4. Construct and return a list of JSON-serializable event dictionaries.

    """

    session_id = str(uuid.uuid4())[:8]

    # splits
    variant = random.choice(["control", "new_checkout_ui"])
    device = random.choice(["mobile", "desktop", "tablet"])
    channel = random.choice(["Organic", "Google_Ads", "Email", "Direct"])

    # initialize session start time
    current_time = datetime.now() - timedelta(minutes=random.randint(1, 300))

    # unpack config variables
    funnel_steps = config["funnel"]["steps"]
    base_probs = config["funnel"]["transition_probabilities"]
    exp_config = config["experiments"]["checkout_conversion"]

    events = []

    for i, step in enumerate(funnel_steps):
        # start at view homepage
        if i == 0:
            should_continue = True

        # evaluate A/B test transition (checkout initation to purchase completion)
        elif funnel_steps[i-1] == "checkout_initated":
            # select underlying popn conversion probability based on the user's assigned group
            prob = exp_config["variant"] if variant == "new_checkout_ui" else exp_config["control"]
            # Monte Carlo decision
            should_continue = random.random() < prob

        # evaluate baseline transition probabilities for standard funnel steps
        else:
            prob = base_probs.get(funnel_steps[i-1], 0.0)
            should_continue = random.random() < prob

        # if user fails prob check, they exit
        if not should_continue:
            break

        # simulate realistic time delay between click (15 to 120 sec)
        current_time += timedelta(seconds=random.randint(15, 120))

        # build the structured telemetry payload
        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": step,
            "timestamp": current_time.isoformat(),
            "device": device,
            "customer_metadata": {
             "acquisition_channel": channel,
             "signup_date": "2026-06-01",
             "experiment_variant": variant   
            }
        }
        events.append(payload)

    return events

def run_generator():
    """
    Generates and persists mock clickstream events
    """
    config = load_config()

    output_dir = config["simulation"]["output_dir"]
    num_users = config["simulation"]["num_users"]

    os.makedirs(output_dir, exists_ok=True)

    total_events = 0
    print(f"Generating clickstream data for {num_users} users...")

    for idx in range(num_users):
        user_id = f"usr_{1000 + idx}"
        session_events = simulate_user_session(user_id, config)

        # save each event as an individual JSON file
        for event in session_events:
            file_path = os.path.join(output_dir, f"event_{event['event_id']}.json")
            with open(file_path, "w") as f:
                json.dump(event, f, indent=2)
            total_events += 1

    print(f"Simulation complete. Generated {total_events} raw event files in '{output_dir}/'.")


if __name__ == "__main__":
    run_generator()