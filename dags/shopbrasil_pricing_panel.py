"""
## ShopBrasil — Painel de preços por categoria

Substitui o script Python agendado via cron.

Topologia:
- **ingestao** (linear): buscar_produtos -> validar_produtos -> extrair_categorias
- **analise** (fan-out / fan-in): calcular_metricas.expand(categoria) -> consolidar
- **persistencia** (linear): criar_tabelas -> salvar_snapshot -> salvar_historico

Idempotência: snapshot = DELETE+INSERT por `reference_date` na mesma transação;
histórico = append com UNIQUE(run_id, category) + ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include import sql
from include.api import fetch_products
from include.callbacks import (
    notify_dag_failure,
    notify_sla_miss,
    notify_task_failure,
    notify_task_retry,
    notify_task_success,
)
from include.config import ECOMMERCE_POOL, POSTGRES_CONN_ID, TIMEZONE
from include.metrics import compute_category_metrics, extract_categories
from include.operators import ValidarProdutosOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="shopbrasil_pricing_panel",
    description="Métricas de preço por categoria (FakeStore API -> PostgreSQL)",
    schedule="0 6 * * *",  # 06:00 no timezone do start_date (America/Sao_Paulo)
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_dag_failure,
    sla_miss_callback=notify_sla_miss,
    tags=["shopbrasil", "pricing"],
    doc_md=__doc__,
)
def shopbrasil_pricing_panel():
    # ------------------------------------------------------------------ ingestão
    @task_group(group_id="ingestao")
    def ingestao():
        @task(
            task_id="buscar_produtos",
            retries=5,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,  # 30s, 60s, 120s, ... até max_retry_delay
            max_retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(minutes=5),
            on_failure_callback=notify_task_failure,
            on_retry_callback=notify_task_retry,
            on_success_callback=notify_task_success,
        )
        def buscar_produtos() -> list[dict[str, Any]]:
            # 20 produtos no FakeStore: cabe no XCom. Para catálogo grande, gravar em
            # staging (S3/tabela) e trafegar só a referência.
            return fetch_products()

        @task(task_id="extrair_categorias")
        def extrair_categorias(produtos: list[dict[str, Any]]) -> list[str]:
            categorias = extract_categories(produtos)
            log.info("Categorias detectadas: %s", categorias)
            return categorias

        produtos = buscar_produtos()
        validados = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos).output
        categorias = extrair_categorias(validados)
        return {"produtos": validados, "categorias": categorias}

    # ------------------------------------------------------------------ análise
    @task_group(group_id="analise")
    def analise(produtos, categorias):
        # Fan-out: 1 task mapeada por categoria; concorrência limitada pelo pool (2 slots)
        @task(task_id="calcular_metricas", pool=ECOMMERCE_POOL)
        def calcular_metricas(produtos: list[dict[str, Any]], categoria: str) -> dict[str, Any]:
            return compute_category_metrics(produtos, categoria)

        # Fan-in: recebe a coleção de resultados de todas as instâncias mapeadas
        @task(task_id="consolidar")
        def consolidar(metricas) -> list[dict[str, Any]]:
            rows = sorted(metricas, key=lambda m: m["category"])
            log.info("Consolidadas %d categorias", len(rows))
            return rows

        mapeadas = calcular_metricas.partial(produtos=produtos).expand(categoria=categorias)
        return consolidar(mapeadas)

    # ------------------------------------------------------------- persistência
    @task_group(group_id="persistencia")
    def persistencia(metricas):
        @task(task_id="criar_tabelas")
        def criar_tabelas() -> None:
            PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql.CREATE_TABLES)

        @task(task_id="salvar_snapshot", sla=timedelta(hours=1))
        def salvar_snapshot(rows: list[dict[str, Any]], data_interval_end=None) -> int:
            ref = data_interval_end.in_timezone(TIMEZONE).date()
            params = [{**r, "reference_date": ref} for r in rows]
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            # `with conn`: commit no sucesso, rollback na exceção -> DELETE+INSERT atômico
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql.DELETE_SNAPSHOT_DATE, {"reference_date": ref})
                cur.executemany(sql.INSERT_SNAPSHOT, params)
            log.info("Snapshot %s: %d linhas", ref, len(params))
            return len(params)

        @task(task_id="salvar_historico")
        def salvar_historico(rows: list[dict[str, Any]], run_id=None, data_interval_end=None) -> None:
            ref = data_interval_end.in_timezone(TIMEZONE).date()
            params = [{**r, "reference_date": ref, "run_id": run_id} for r in rows]
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.executemany(sql.INSERT_HISTORY, params)

        tabelas = criar_tabelas()
        snap = salvar_snapshot(metricas)
        hist = salvar_historico(metricas)
        tabelas >> snap >> hist

    ing = ingestao()
    metricas = analise(ing["produtos"], ing["categorias"])
    persistencia(metricas)


shopbrasil_pricing_panel()
