import subprocess
import pytest

@pytest.mark.system
def test_full_pipeline_execution():
    """
    Executes generator -> streamer -> dbt build -> evaluation script in sequence and asserts zero exit code errors.
    """
    # Run event generator for a micro-sample (50 users)
    gen_result = subprocess.run(["python3", "src/pipeline/event_generator.py"], capture_output = True, text = True
    )
    assert gen_result.returncode == 0, f"Generator failed: {gen_result.stderr}"

    # Run dbt build in Sandbox Target
    dbt_result = subprocess.run(
        ["dbt", "build", "--target", "dev"],
        capture_ouput=True, text=True
    )
    assert dbt_result.returncode == 0, f"dbt build failed: {dbt_result.stderr}"

    # Step 3: Run Statistical Evaluation Script
    eval_result = subprocess.run(
        ["python3", "scripts/evaluate_experiment.py"],
        capture_output=True, text=True
    )
    assert eval_result.returncode == 0, f"Evaluation script failed: {eval_result.stderr}"