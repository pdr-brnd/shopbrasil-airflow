"""Callbacks de ciclo de vida. Em produção, trocar o log por Slack/e-mail/PagerDuty."""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def _ti_info(context: dict[str, Any]) -> str:
    ti = context["task_instance"]
    return f"dag={ti.dag_id} task={ti.task_id} run_id={context['run_id']} try={ti.try_number}"


def notify_task_success(context: dict[str, Any]) -> None:
    log.info("[SUCESSO] %s", _ti_info(context))


def notify_task_retry(context: dict[str, Any]) -> None:
    exc = context.get("exception")
    log.warning("[RETRY] %s | próxima tentativa agendada | erro=%r", _ti_info(context), exc)


def notify_task_failure(context: dict[str, Any]) -> None:
    exc = context.get("exception")
    # Simulação de alerta (ex.: webhook do Slack)
    log.error("[ALERTA SIMULADO] Task falhou definitivamente: %s | erro=%r", _ti_info(context), exc)


def notify_dag_failure(context: dict[str, Any]) -> None:
    log.error("[ALERTA SIMULADO] DAG falhou: dag=%s run_id=%s", context["dag"].dag_id, context["run_id"])


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.error("[ALERTA SIMULADO] SLA estourado em %s: %s", dag.dag_id, task_list)
