"""
Agentic AI Pipeline — Multi-Step ReAct Agent using Groq

Implements the ReAct (Reasoning + Acting) pattern:
  1. THINK: LLM reasons about what to do next
  2. ACT: Execute a tool (web search, calculator, code runner, etc.)
  3. OBSERVE: Feed tool output back to LLM
  4. Repeat until task is complete or max_steps reached

Full observability:
  - Per-step latency and status in Prometheus
  - Agent session traces in Jaeger (parent span + child spans per step)
  - Structured logs: every step, tool call, and decision logged
  - Kafka events: session summaries for downstream analytics
  - Loop detection: alerts when agents hit max_steps

Run standalone:
  GROQ_API_KEY=gsk_... uvicorn agent:app --port 8003
"""

import os
import sys
import time
import json
import random
import asyncio
from pathlib import Path
from typing import Optional, Any
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import groq
from kafka import KafkaProducer

from common.metrics import (
    AGENT_SESSIONS, AGENT_SESSION_DURATION, AGENT_STEPS, AGENT_STEP_DURATION,
    AGENT_TOOL_CALLS, AGENT_MAX_STEPS_REACHED, AGENT_HALLUCINATIONS, AGENT_QUEUE_DEPTH,
    GENAI_REQUESTS, GENAI_REQUEST_DURATION, GENAI_TOKENS, GENAI_RATE_LIMIT_HITS,
)
from common.logging_config import configure_logging, get_logger
from common.tracing import configure_tracing

# ── Bootstrap ─────────────────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "agentic-pipeline")
configure_logging(SERVICE_NAME, os.getenv("LOG_LEVEL", "INFO"))
tracer = configure_tracing(SERVICE_NAME)
logger = get_logger(__name__)

app = FastAPI(title="Agentic AI Pipeline", version="1.0.0")

# ── Groq client ───────────────────────────────────────────────────
def get_groq_client() -> groq.Groq:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or api_key == "your_groq_api_key_here":
        raise RuntimeError("GROQ_API_KEY not set.")
    return groq.Groq(api_key=api_key)


# ── Kafka producer ─────────────────────────────────────────────────
_kafka_producer: Optional[KafkaProducer] = None

def get_kafka_producer() -> Optional[KafkaProducer]:
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000,
            )
        except Exception as e:
            logger.warning("kafka_unavailable", error=str(e))
    return _kafka_producer


# ── Tool definitions (what the agent can call) ────────────────────

class ToolResult(BaseModel):
    tool_name: str
    success: bool
    result: str
    duration_ms: float


def tool_web_search(query: str) -> ToolResult:
    """Simulated web search tool."""
    start = time.time()
    # Simulate latency + occasional failures
    time.sleep(random.uniform(0.1, 0.8))
    if random.random() < 0.05:   # 5% failure rate
        raise RuntimeError(f"Search service timeout for query: {query}")

    results = [
        f"Search result 1 for '{query}': According to recent studies...",
        f"Search result 2 for '{query}': Experts suggest that...",
        f"Search result 3 for '{query}': The latest data shows...",
    ]
    return ToolResult(
        tool_name="web_search",
        success=True,
        result="\n".join(results),
        duration_ms=(time.time() - start) * 1000,
    )


def tool_calculator(expression: str) -> ToolResult:
    """Safe math calculator tool."""
    start = time.time()
    try:
        # Only allow safe math expressions
        allowed = set("0123456789+-*/()., ")
        if not all(c in allowed for c in expression):
            raise ValueError("Expression contains disallowed characters")
        result = eval(expression, {"__builtins__": {}})
        return ToolResult(
            tool_name="calculator",
            success=True,
            result=str(result),
            duration_ms=(time.time() - start) * 1000,
        )
    except Exception as e:
        return ToolResult(
            tool_name="calculator",
            success=False,
            result=f"Error: {e}",
            duration_ms=(time.time() - start) * 1000,
        )


def tool_get_current_metrics(service: str) -> ToolResult:
    """Tool to query current observability metrics (simulated)."""
    start = time.time()
    metrics = {
        "genai-pipeline": {
            "p99_latency_ms": round(random.uniform(800, 4000), 1),
            "error_rate": round(random.uniform(0.01, 0.08), 3),
            "requests_per_second": round(random.uniform(0.5, 5.0), 2),
            "active_model": "llama-3.1-8b-instant",
        },
        "ml-pipeline": {
            "inference_p99_ms": round(random.uniform(10, 500), 1),
            "model_accuracy": round(random.uniform(0.75, 0.95), 3),
            "data_drift_score": round(random.uniform(0.0, 0.4), 3),
        },
    }
    result = metrics.get(service, {"error": f"Unknown service: {service}"})
    return ToolResult(
        tool_name="get_current_metrics",
        success=True,
        result=json.dumps(result, indent=2),
        duration_ms=(time.time() - start) * 1000,
    )


AVAILABLE_TOOLS = {
    "web_search": tool_web_search,
    "calculator": tool_calculator,
    "get_current_metrics": tool_get_current_metrics,
}

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information about a topic.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculator",
            "description": "Evaluate a mathematical expression.",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {"type": "string", "description": "Math expression to evaluate"}
                },
                "required": ["expression"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_metrics",
            "description": "Get current performance metrics for a service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service": {"type": "string", "description": "Service name, e.g. genai-pipeline"}
                },
                "required": ["service"],
            },
        },
    },
]


# ── Agent request/response models ─────────────────────────────────
class AgentRequest(BaseModel):
    task: str
    agent_name: str = "research-agent"
    model: str = "llama-3.3-70b-versatile"  # use bigger model for reasoning
    max_steps: int = 10
    session_id: Optional[str] = None


class StepRecord(BaseModel):
    step: int
    reasoning: Optional[str]
    tool_name: Optional[str]
    tool_input: Optional[dict]
    tool_result: Optional[str]
    duration_ms: float


class AgentResponse(BaseModel):
    session_id: str
    agent_name: str
    task: str
    final_answer: str
    steps_taken: int
    total_duration_ms: float
    status: str
    step_trace: list[StepRecord]


# ── Core Agent Loop ────────────────────────────────────────────────
async def run_agent(req: AgentRequest) -> AgentResponse:
    """
    ReAct agent loop with full observability.

    Each iteration:
      1. Send current message history to LLM
      2. If LLM calls a tool → execute it, record result, continue
      3. If LLM gives a final answer → return
      4. If max_steps reached → terminate with warning
    """
    session_id = req.session_id or f"session-{int(time.time() * 1000)}"
    session_start = time.time()
    steps: list[StepRecord] = []
    status = "completed"

    AGENT_QUEUE_DEPTH.labels(service=SERVICE_NAME).inc()

    with tracer.start_as_current_span("agent_session") as session_span:
        session_span.set_attribute("agent.name", req.agent_name)
        session_span.set_attribute("agent.task", req.task)
        session_span.set_attribute("agent.max_steps", req.max_steps)
        session_span.set_attribute("session.id", session_id)

        logger.info(
            "agent_session_started",
            session_id=session_id,
            agent_name=req.agent_name,
            task=req.task[:100],
            model=req.model,
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a helpful research agent. Use the available tools to gather "
                    "information and provide accurate, well-reasoned answers. "
                    "Think step by step. When you have enough information, provide a final answer."
                ),
            },
            {"role": "user", "content": req.task},
        ]

        client = get_groq_client()
        final_answer = "Agent did not produce a final answer."

        try:
            for step_num in range(1, req.max_steps + 1):
                step_start = time.time()

                with tracer.start_as_current_span(f"agent_step_{step_num}") as step_span:
                    step_span.set_attribute("step.number", step_num)

                    AGENT_STEPS.labels(
                        service=SERVICE_NAME, agent_name=req.agent_name
                    ).inc()

                    # ── LLM call ──────────────────────────────────
                    try:
                        llm_start = time.time()
                        response = client.chat.completions.create(
                            model=req.model,
                            messages=messages,
                            tools=TOOL_SCHEMAS,
                            tool_choice="auto",
                            max_tokens=1024,
                            temperature=0.1,
                        )
                        llm_latency = (time.time() - llm_start) * 1000

                        GENAI_REQUESTS.labels(
                            service=SERVICE_NAME, model=req.model, status="success"
                        ).inc()
                        GENAI_REQUEST_DURATION.labels(
                            service=SERVICE_NAME, model=req.model
                        ).observe(llm_latency / 1000)
                        if response.usage:
                            GENAI_TOKENS.labels(
                                service=SERVICE_NAME, model=req.model, type="input"
                            ).inc(response.usage.prompt_tokens)
                            GENAI_TOKENS.labels(
                                service=SERVICE_NAME, model=req.model, type="output"
                            ).inc(response.usage.completion_tokens)

                    except groq.RateLimitError:
                        GENAI_RATE_LIMIT_HITS.labels(
                            service=SERVICE_NAME, model=req.model
                        ).inc()
                        logger.warning(
                            "agent_llm_rate_limited",
                            session_id=session_id, step=step_num
                        )
                        await asyncio.sleep(5)
                        continue

                    choice = response.choices[0]
                    tool_calls = choice.message.tool_calls
                    step_duration = (time.time() - step_start) * 1000

                    AGENT_STEP_DURATION.labels(
                        service=SERVICE_NAME, agent_name=req.agent_name
                    ).observe(step_duration / 1000)

                    # ── Final answer (no tool call) ───────────────
                    if not tool_calls:
                        final_answer = choice.message.content or final_answer
                        step_span.set_attribute("step.type", "final_answer")

                        steps.append(StepRecord(
                            step=step_num,
                            reasoning=final_answer[:200],
                            tool_name=None,
                            tool_input=None,
                            tool_result=None,
                            duration_ms=step_duration,
                        ))

                        logger.info(
                            "agent_final_answer",
                            session_id=session_id,
                            step=step_num,
                            answer_preview=final_answer[:100],
                        )
                        break

                    # ── Tool call ─────────────────────────────────
                    tool_call = tool_calls[0]
                    tool_name = tool_call.function.name
                    try:
                        tool_args = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        tool_args = {}

                    step_span.set_attribute("step.type", "tool_call")
                    step_span.set_attribute("tool.name", tool_name)

                    logger.info(
                        "agent_tool_call",
                        session_id=session_id,
                        step=step_num,
                        tool_name=tool_name,
                        tool_args=str(tool_args)[:200],
                    )

                    # Execute the tool
                    tool_result_str = "Tool not found"
                    tool_success = False

                    if tool_name in AVAILABLE_TOOLS:
                        try:
                            tool_fn = AVAILABLE_TOOLS[tool_name]
                            first_arg = next(iter(tool_args.values()), "")
                            tool_result = tool_fn(first_arg)
                            tool_result_str = tool_result.result
                            tool_success = tool_result.success

                            AGENT_TOOL_CALLS.labels(
                                service=SERVICE_NAME,
                                agent_name=req.agent_name,
                                tool_name=tool_name,
                                status="success" if tool_success else "error",
                            ).inc()

                        except Exception as e:
                            tool_result_str = f"Tool error: {e}"
                            AGENT_TOOL_CALLS.labels(
                                service=SERVICE_NAME,
                                agent_name=req.agent_name,
                                tool_name=tool_name,
                                status="error",
                            ).inc()
                            logger.error(
                                "agent_tool_error",
                                session_id=session_id,
                                step=step_num,
                                tool_name=tool_name,
                                error=str(e),
                            )
                    else:
                        AGENT_TOOL_CALLS.labels(
                            service=SERVICE_NAME,
                            agent_name=req.agent_name,
                            tool_name=tool_name,
                            status="not_found",
                        ).inc()

                    # Add assistant message + tool result to history
                    messages.append({"role": "assistant", "content": None,
                                     "tool_calls": [tool_call.model_dump()]})
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": tool_result_str,
                    })

                    steps.append(StepRecord(
                        step=step_num,
                        reasoning=None,
                        tool_name=tool_name,
                        tool_input=tool_args,
                        tool_result=tool_result_str[:300],
                        duration_ms=step_duration,
                    ))

            else:
                # max_steps exhausted without final answer
                status = "max_steps_reached"
                AGENT_MAX_STEPS_REACHED.labels(
                    service=SERVICE_NAME, agent_name=req.agent_name
                ).inc()
                logger.warning(
                    "agent_max_steps_reached",
                    session_id=session_id,
                    max_steps=req.max_steps,
                    task_preview=req.task[:100],
                )
                final_answer = (
                    f"Task exceeded {req.max_steps} steps without completion. "
                    "This may indicate an agent loop. Review the step trace."
                )

        except Exception as e:
            status = "failed"
            session_span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(
                "agent_session_failed",
                session_id=session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            final_answer = f"Agent failed with error: {e}"

        finally:
            AGENT_QUEUE_DEPTH.labels(service=SERVICE_NAME).dec()

        session_duration_ms = (time.time() - session_start) * 1000

        AGENT_SESSIONS.labels(
            service=SERVICE_NAME, agent_name=req.agent_name, status=status
        ).inc()
        AGENT_SESSION_DURATION.labels(
            service=SERVICE_NAME, agent_name=req.agent_name
        ).observe(session_duration_ms / 1000)

        logger.info(
            "agent_session_completed",
            session_id=session_id,
            agent_name=req.agent_name,
            steps_taken=len(steps),
            duration_ms=round(session_duration_ms, 2),
            status=status,
        )

        # Emit Kafka event for analytics
        producer = get_kafka_producer()
        if producer:
            producer.send("agent-events", {
                "event_type": "session_completed",
                "session_id": session_id,
                "agent_name": req.agent_name,
                "steps_taken": len(steps),
                "duration_ms": round(session_duration_ms, 2),
                "status": status,
                "timestamp": time.time(),
            })

        return AgentResponse(
            session_id=session_id,
            agent_name=req.agent_name,
            task=req.task,
            final_answer=final_answer,
            steps_taken=len(steps),
            total_duration_ms=round(session_duration_ms, 2),
            status=status,
            step_trace=steps,
        )


# ── FastAPI Routes ─────────────────────────────────────────────────
@app.post("/run", response_model=AgentResponse)
async def run(req: AgentRequest):
    return await run_agent(req)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


# ── Demo tasks for load generation ────────────────────────────────
DEMO_TASKS = [
    "Research and explain how the transformer architecture works.",
    "Calculate compound interest on $10,000 at 7% over 10 years, then search for comparison.",
    "What are the current best practices for LLM observability in production?",
    "Explain the CAP theorem and its implications for distributed systems.",
    "Analyze the tradeoffs between Kafka and RabbitMQ for event streaming.",
    "What are the key metrics to monitor in a GenAI pipeline?",
    "Explain how Prometheus scraping works with pull vs push models.",
    "What is the difference between latency and throughput in distributed systems?",
]


async def demo_load_generator():
    """Runs background agent tasks for demo purposes."""
    await asyncio.sleep(10)  # Wait for all services to be ready
    while True:
        await asyncio.sleep(random.uniform(15, 45))  # Agents run less frequently
        task = random.choice(DEMO_TASKS)
        req = AgentRequest(
            task=task,
            agent_name=random.choice(["research-agent", "analysis-agent"]),
            max_steps=random.randint(3, 8),
        )
        try:
            await run_agent(req)
        except Exception as e:
            logger.warning("demo_agent_failed", error=str(e))


@app.on_event("startup")
async def startup():
    logger.info("agentic_pipeline_starting", service=SERVICE_NAME)
    if os.getenv("DEMO_MODE", "true").lower() == "true":
        asyncio.create_task(demo_load_generator())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
