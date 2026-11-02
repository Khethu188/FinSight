
# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration Helpers
# MAGIC
# MAGIC Shared utilities for task communication, timing, and workflow control.

# COMMAND ----------

import json
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class TaskMetrics:
    """Standardized metrics passed between workflow tasks."""
    task_name: str
    run_id: str
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: Optional[float] = None
    records_input: int = 0
    records_output: int = 0
    records_quarantined: int = 0
    records_deduplicated: int = 0
    status: str = "running"
    error_message: Optional[str] = None
    custom_metrics: Optional[dict] = None

    def complete(self, records_output: int, **kwargs):
        """Mark task as completed with output metrics."""
        self.end_time = datetime.now().isoformat()
        self.duration_seconds = round(
            (datetime.fromisoformat(self.end_time) -
             datetime.fromisoformat(self.start_time)).total_seconds(), 2
        )
        self.records_output = records_output
        self.status = "completed"
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def fail(self, error: str):
        """Mark task as failed."""
        self.end_time = datetime.now().isoformat()
        self.duration_seconds = round(
            (datetime.fromisoformat(self.end_time) -
             datetime.fromisoformat(self.start_time)).total_seconds(), 2
        )
        self.status = "failed"
        self.error_message = error

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "TaskMetrics":
        return cls(**json.loads(json_str))


class WorkflowContext:
    """
    Manages workflow-level context and inter-task communication
    using Databricks task values.
    """

    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self._start_time = time.time()

    def start_task(self, task_name: str, records_input: int = 0) -> TaskMetrics:
        """Initialize metrics for a new task."""
        metrics = TaskMetrics(
            task_name=task_name,
            run_id=self.run_id,
            start_time=datetime.now().isoformat(),
            records_input=records_input,
        )
        print(f"\n{'=' * 65}")
        print(f"  ▶ STARTING: {task_name}")
        print(f"  Run ID: {self.run_id}")
        print(f"  Input records: {records_input:,}")
        print(f"{'=' * 65}\n")
        return metrics

    def complete_task(self, metrics: TaskMetrics, records_output: int, **kwargs):
        """Finalize task metrics and publish via task values."""
        metrics.complete(records_output, **kwargs)

        # Publish metrics for downstream tasks
        try:
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_metrics",
                value=metrics.to_json(),
            )
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_record_count",
                value=str(records_output),
            )
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_status",
                value=metrics.status,
            )
        except Exception:
            pass  # Running outside a workflow (e.g., interactive)

        print(f"\n{'=' * 65}")
        print(f"  ✓ COMPLETED: {metrics.task_name}")
        print(f"  Duration    : {metrics.duration_seconds:.1f}s")
        print(f"  Input       : {metrics.records_input:,}")
        print(f"  Output      : {metrics.records_output:,}")
        if metrics.records_quarantined:
            print(f"  Quarantined : {metrics.records_quarantined:,}")
        if metrics.records_deduplicated:
            print(f"  Deduplicated: {metrics.records_deduplicated:,}")
        print(f"{'=' * 65}\n")

        return metrics

    def fail_task(self, metrics: TaskMetrics, error: str):
        """Record task failure and publish error."""
        metrics.fail(error)

        try:
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_metrics",
                value=metrics.to_json(),
            )
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_status",
                value="failed",
            )
            dbutils.jobs.taskValues.set(
                key=f"{metrics.task_name}_error",
                value=error[:500],
            )
        except Exception:
            pass

        print(f"\n{'=' * 65}")
        print(f"  ✗ FAILED: {metrics.task_name}")
        print(f"  Error: {error}")
        print(f"{'=' * 65}\n")

    def get_upstream_metrics(self, task_name: str) -> Optional[TaskMetrics]:
        """Retrieve metrics from an upstream task."""
        try:
            json_str = dbutils.jobs.taskValues.get(
                taskKey=task_name,
                key=f"{task_name}_metrics",
            )
            return TaskMetrics.from_json(json_str)
        except Exception:
            return None

    def get_upstream_count(self, task_name: str) -> int:
        """Get record count from an upstream task."""
        try:
            return int(dbutils.jobs.taskValues.get(
                taskKey=task_name,
                key=f"{task_name}_record_count",
                default="0",
            ))
        except Exception:
            return 0

    def check_upstream_health(self, task_name: str) -> bool:
        """Check if an upstream task completed successfully."""
        try:
            status = dbutils.jobs.taskValues.get(
                taskKey=task_name,
                key=f"{task_name}_status",
                default="unknown",
            )
            return status == "completed"
        except Exception:
            return True  # Assume healthy if running interactively

    def get_workflow_summary(self, task_names: list[str]) -> dict:
        """Build a summary of all tasks in the workflow."""
        summary = {
            "run_id": self.run_id,
            "total_duration_seconds": round(time.time() - self._start_time, 2),
            "tasks": {},
        }

        for name in task_names:
            metrics = self.get_upstream_metrics(name)
            if metrics:
                summary["tasks"][name] = {
                    "status": metrics.status,
                    "duration": metrics.duration_seconds,
                    "input": metrics.records_input,
                    "output": metrics.records_output,
                }

        return summary

