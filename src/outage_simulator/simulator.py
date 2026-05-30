"""
Production Outage Simulator

Injects realistic failure modes into the pipeline stack so you can:
  - Practice reading Prometheus alerts and Grafana dashboards
  - Follow runbooks to diagnose and resolve each scenario
  - Understand what each alert looks like in real data

Usage:
  python simulator.py --scenario <name> [--duration 60]

Available scenarios:
  1. llm_rate_limit       → Groq API rate limits spike
  2. agent_loop           → Agents stuck hitting max_steps
  3. high_latency         → LLM response times degrade
  4. ml_data_drift        → Feature distribution shifts
  5. kafka_lag            → Consumer falls behind producers
  6. model_accuracy_drop  → ML model accuracy degrades
  7. cascade_failure      → Multiple services fail together
"""

import argparse
import asyncio
import os
import random
import time
import httpx
import sys
import json
from typing import Optional

GENAI_URL = os.environ.get("GENAI_PIPELINE_URL", "http://localhost:8001")
ML_URL = os.environ.get("ML_PIPELINE_URL", "http://localhost:8002")
AGENTIC_URL = os.environ.get("AGENTIC_PIPELINE_URL", "http://localhost:8003")


async def scenario_llm_rate_limit(duration_seconds: int = 60):
    """
    SCENARIO: Groq API Rate Limit Storm
    ─────────────────────────────────────
    What happens: Burst of requests exhausts Groq API rate limits.
    Alert fires: GenAIRateLimitHigh (warning), GenAIHighErrorRate (critical)
    How to diagnose:
      - Grafana: GenAI dashboard → rate_limit_hits_total spike
      - Prometheus: rate(genai_rate_limit_hits_total[5m])
      - Kibana: filter logs by error_type="rate_limit_error"
    How to fix:
      - Implement exponential backoff + jitter
      - Add request queue with rate limiting
      - Consider Groq tier upgrade or model fallback
    """
    print(f"\n[SCENARIO] LLM Rate Limit Storm — running for {duration_seconds}s")
    print("  Watch: Grafana → GenAI Dashboard → Rate Limit Hits")
    print("  Alert: GenAIRateLimitHigh fires after ~2 minutes\n")

    end_time = time.time() + duration_seconds
    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.time() < end_time:
            # Burst of concurrent requests
            tasks = [
                client.post(f"{GENAI_URL}/infer", json={
                    "prompt": "What is quantum computing? Explain in detail.",
                    "model": "llama-3.3-70b-versatile",
                    "max_tokens": 500,
                    "request_id": f"rate-limit-test-{i}",
                })
                for i in range(10)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            rate_limits = sum(1 for r in results if hasattr(r, 'status_code') and r.status_code == 429)
            errors = sum(1 for r in results if isinstance(r, Exception))
            print(f"  Batch sent: {len(tasks)} | Rate limited: {rate_limits} | Errors: {errors}")
            await asyncio.sleep(2)


async def scenario_agent_loop(duration_seconds: int = 120):
    """
    SCENARIO: Agent Infinite Loop
    ──────────────────────────────
    What happens: Tasks designed to make agents take maximum steps.
    Alert fires: AgentMaxStepsReached (critical)
    How to diagnose:
      - Prometheus: rate(agent_max_steps_reached_total[10m]) > 0.1
      - Jaeger: Find spans with step count = max_steps
      - Kibana: agent_steps > 10, search for repeated tool calls
    How to fix:
      - Review agent prompts for circular reasoning
      - Add step-back prompting: "Are you repeating yourself?"
      - Implement tool-call deduplication
      - Reduce max_steps for this agent type
    """
    print(f"\n[SCENARIO] Agent Infinite Loop — running for {duration_seconds}s")
    print("  Watch: Grafana → Agentic Dashboard → Max Steps Reached")
    print("  Alert: AgentMaxStepsReached fires after ~2 minutes\n")

    # Tasks that encourage circular reasoning
    loopy_tasks = [
        "Search for the best search engine to use for searching for information about searching.",
        "Calculate the result of the calculation of calculating 2+2, then verify the calculation.",
        "Research how to research the best way to research observability metrics.",
    ]

    end_time = time.time() + duration_seconds
    async with httpx.AsyncClient(timeout=120.0) as client:
        while time.time() < end_time:
            task = random.choice(loopy_tasks)
            try:
                resp = await client.post(f"{AGENTIC_URL}/run", json={
                    "task": task,
                    "agent_name": "loop-test-agent",
                    "max_steps": 3,   # Low max to trigger alert quickly
                    "model": "llama-3.1-8b-instant",
                })
                data = resp.json()
                print(f"  Session: {data.get('session_id', '?')} | "
                      f"Steps: {data.get('steps_taken', '?')} | "
                      f"Status: {data.get('status', '?')}")
            except Exception as e:
                print(f"  Error: {e}")
            await asyncio.sleep(5)


async def scenario_high_latency(duration_seconds: int = 180):
    """
    SCENARIO: LLM Latency Spike
    ─────────────────────────────
    What happens: Requests to slow/large models cause P99 to breach SLO.
    Alert fires: GenAIHighLatency (warning)
    How to diagnose:
      - Prometheus: histogram_quantile(0.99, rate(genai_request_duration_seconds_bucket[5m]))
      - Grafana: GenAI Dashboard → Latency Heatmap
      - Jaeger: Filter traces by duration > 5s
    How to fix:
      - Switch to smaller/faster model (llama3-8b vs 70b)
      - Reduce max_tokens
      - Add streaming responses
      - Implement request timeout + fallback
    """
    print(f"\n[SCENARIO] High Latency Injection — running for {duration_seconds}s")
    print("  Watch: Grafana → GenAI Dashboard → P99 Latency")
    print("  Alert: GenAIHighLatency fires after ~3 minutes\n")

    end_time = time.time() + duration_seconds
    async with httpx.AsyncClient(timeout=120.0) as client:
        while time.time() < end_time:
            # Use large model + max tokens to maximize latency
            start = time.time()
            try:
                resp = await client.post(f"{GENAI_URL}/infer", json={
                    "prompt": "Write a comprehensive 2000-word essay on the history of artificial intelligence, covering all major milestones from the 1950s to today.",
                    "model": "llama-3.3-70b-versatile",
                    "max_tokens": 2048,
                })
                latency = (time.time() - start) * 1000
                print(f"  Request completed: {latency:.0f}ms | Status: {resp.status_code}")
            except httpx.TimeoutException:
                print(f"  Request TIMED OUT after {(time.time() - start):.1f}s")
            except Exception as e:
                print(f"  Error: {e}")
            await asyncio.sleep(3)


async def scenario_ml_data_drift(duration_seconds: int = 120):
    """
    SCENARIO: ML Model Data Drift
    ──────────────────────────────
    What happens: Transaction distribution shifts — amounts become 10x larger.
    Alert fires: MLDataDriftDetected (warning)
    How to diagnose:
      - Prometheus: ml_data_drift_score > 0.3
      - Grafana: ML Pipeline → Data Drift Score by feature
      - Kibana: logs with anomaly="high_latency" tag
    How to fix:
      - Identify which features drifted (KS-test scores)
      - Check upstream data pipeline for schema changes
      - Trigger model retraining with recent data
      - Add feature monitoring with alerts per feature
    """
    print(f"\n[SCENARIO] ML Data Drift — running for {duration_seconds}s")
    print("  Watch: Grafana → ML Pipeline → Data Drift Score")
    print("  Alert: MLDataDriftDetected fires when drift_score > 0.3\n")

    end_time = time.time() + duration_seconds
    async with httpx.AsyncClient(timeout=10.0) as client:
        while time.time() < end_time:
            # Inject heavily drifted transactions
            resp = await client.post(f"{ML_URL}/predict", json={
                "transaction_id": f"drift-test-{int(time.time() * 1000)}",
                "transaction_amount": random.uniform(50000, 500000),  # 100x normal
                "hour_of_day": random.randint(0, 23),
                "day_of_week": random.randint(0, 6),
                "merchant_category": random.randint(0, 19),
                "user_age_days": random.randint(1, 30),  # All new users (drifted)
                "tx_count_24h": random.randint(50, 200), # High frequency (drifted)
                "amount_deviation": random.uniform(10, 50),
                "is_international": True,  # All international (drifted)
                "device_trust_score": random.uniform(0.0, 0.2), # Low trust (drifted)
            })
            data = resp.json()
            print(f"  tx: {data['transaction_id']} | "
                  f"fraud_prob: {data['fraud_probability']:.3f} | "
                  f"confidence: {data['confidence']:.3f}")
            await asyncio.sleep(0.1)


async def scenario_kafka_lag(duration_seconds: int = 120):
    """
    SCENARIO: Kafka Consumer Lag
    ─────────────────────────────
    What happens: Messages accumulate faster than they're consumed.
    Alert fires: KafkaConsumerLagHigh (critical)
    How to diagnose:
      - Prometheus: kafka_consumergroup_lag > 10000
      - Kafka UI: http://localhost:8080 → Consumer Groups → Lag
      - Grafana: Kafka Dashboard → Consumer Lag by Topic/Partition
    How to fix:
      - Scale consumer instances (add more pods/threads)
      - Check consumer processing time per message
      - Review if upstream produce rate spiked
      - Temporarily pause non-critical consumers
    """
    print(f"\n[SCENARIO] Kafka Consumer Lag Simulation — {duration_seconds}s")
    print("  Watch: Kafka UI at http://localhost:8080")
    print("  Also: Grafana → Kafka Dashboard → Consumer Lag\n")
    print("  NOTE: This scenario shows you WHERE to look.")
    print("  In real life: stop consumer pods, then monitor lag building up.\n")

    for i in range(10):
        print(f"  Step {i+1}: Check kafka_consumergroup_lag metric in Prometheus")
        print(f"    Query: kafka_consumergroup_lag{{topic='pipeline-events'}}")
        await asyncio.sleep(duration_seconds // 10)


async def scenario_cascade_failure(duration_seconds: int = 120):
    """
    SCENARIO: Cascading Failure
    ────────────────────────────
    What happens: Multiple components fail together (realistic production incident).
    Pattern: Kafka lag → consumers slow → GenAI requests queue → rate limits spike
    How to diagnose:
      - Start with the first alert that fired (usually infra or kafka)
      - Trace dependencies: which service feeds which?
      - Use Jaeger to see where requests are dying
    """
    print(f"\n[SCENARIO] Cascade Failure — running for {duration_seconds}s")
    print("  This scenario runs multiple failures simultaneously.")
    print("  Practice: correlate alerts across dashboards\n")

    await asyncio.gather(
        scenario_llm_rate_limit(duration_seconds // 2),
        scenario_ml_data_drift(duration_seconds // 2),
    )


SCENARIOS = {
    "llm_rate_limit": scenario_llm_rate_limit,
    "agent_loop": scenario_agent_loop,
    "high_latency": scenario_high_latency,
    "ml_data_drift": scenario_ml_data_drift,
    "kafka_lag": scenario_kafka_lag,
    "cascade_failure": scenario_cascade_failure,
}


def print_menu():
    print("\n" + "=" * 60)
    print("  PRODUCTION OUTAGE SIMULATOR")
    print("=" * 60)
    print("\nAvailable scenarios:\n")
    for i, (name, fn) in enumerate(SCENARIOS.items(), 1):
        doc = fn.__doc__ or ""
        first_line = doc.strip().split("\n")[1].strip() if "\n" in doc else ""
        print(f"  {i}. {name:25} — {first_line}")
    print("\nUsage: python simulator.py --scenario <name> --duration <seconds>")
    print("       python simulator.py --scenario all\n")


async def main():
    parser = argparse.ArgumentParser(description="Production Outage Simulator")
    parser.add_argument("--scenario", default="menu",
                        help="Scenario name or 'all' to run all")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration per scenario in seconds")
    args = parser.parse_args()

    if args.scenario == "menu" or args.scenario not in SCENARIOS and args.scenario != "all":
        print_menu()
        return

    if args.scenario == "all":
        for name, fn in SCENARIOS.items():
            print(f"\n{'='*60}")
            print(f"Running scenario: {name}")
            await fn(args.duration)
            print(f"Scenario '{name}' complete. Check dashboards!")
            await asyncio.sleep(5)
    else:
        fn = SCENARIOS[args.scenario]
        await fn(args.duration)


if __name__ == "__main__":
    asyncio.run(main())
