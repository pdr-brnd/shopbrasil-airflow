# ShopBrasil — Pipeline de preços (Airflow)

FakeStore API → métricas por categoria → PostgreSQL. Substitui o script via cron.

## Subir
```bash
docker compose up --build -d
# UI: http://localhost:8080  (admin / admin)
# Banco analítico: localhost:5433  (analytics / analytics, db analytics)
```
Despause e dispare a DAG `shopbrasil_pricing_panel`. Agenda: todo dia 06:00 America/Sao_Paulo.

## Conferir resultado
```bash
docker compose exec analytics-db psql -U analytics -c "SELECT * FROM category_price_snapshot ORDER BY 1,2;"
```
Re-dispare/limpe a run e confira que o snapshot não duplica.

## Requisitos → onde está
| Requisito | Implementação |
|---|---|
| TaskFlow, XCom via return | `dags/shopbrasil_pricing_panel.py` |
| Linear / fan-out / fan-in | ingestão (linear) → `calcular_metricas.expand` → `consolidar` |
| Timezone, catchup=False | `pendulum.datetime(..., tz="America/Sao_Paulo")` |
| Retry + exponential backoff, try/except + raise | `buscar_produtos` + `include/api.py` |
| Callbacks success/retry/failure | `include/callbacks.py` |
| Dynamic Task Mapping + Pool (2 slots) | `ecommerce_pool` criado no `airflow-init` |
| TaskGroups | `ingestao`, `analise`, `persistencia` |
| PostgresHook + Connection | `AIRFLOW_CONN_POSTGRES_ANALYTICS` no compose |
| Idempotência | snapshot: DELETE+INSERT por data (transação única) |
| Opcionais | `ValidarProdutosOperator`, tabela histórico (append, UNIQUE run_id+category), SLA + `on_failure_callback` |

## Testes
```bash
pip install "apache-airflow==2.10.5" pytest && PYTHONPATH=. pytest tests
```
