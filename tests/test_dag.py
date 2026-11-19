import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag


def test_dag_loads_and_topology():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["shopbrasil_pricing_panel"]
    assert dag.catchup is False
    assert str(dag.timezone) == "America/Sao_Paulo"
    assert {tg for tg in dag.task_group.children if tg.endswith("ingestao") or tg.endswith("analise") or tg.endswith("persistencia")} == {"ingestao", "analise", "persistencia"}
    calc = dag.get_task("analise.calcular_metricas")
    assert calc.pool == "ecommerce_pool"
    assert type(calc).__name__ == "DecoratedMappedOperator"
    busca = dag.get_task("ingestao.buscar_produtos")
    assert busca.retry_exponential_backoff and busca.retries >= 3
    assert busca.on_failure_callback and busca.on_retry_callback and busca.on_success_callback
    assert "persistencia.salvar_snapshot" in dag.get_task("analise.consolidar").downstream_task_ids
