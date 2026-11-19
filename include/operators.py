"""Operador customizado (requisito opcional)."""
from __future__ import annotations

from numbers import Number
from typing import Any, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

REQUIRED_FIELDS: dict[str, type | tuple[type, ...]] = {
    "id": int,
    "title": str,
    "price": Number,
    "category": str,
}


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    Falha com AirflowFailException (sem retry): dado fora do contrato não melhora tentando de novo.
    Retorna a lista original, que vira XCom para as próximas tasks.
    """

    template_fields: Sequence[str] = ("produtos",)

    def __init__(self, *, produtos: list[dict[str, Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: Any) -> list[dict[str, Any]]:
        errors: list[str] = []
        for idx, p in enumerate(self.produtos):
            for field, expected in REQUIRED_FIELDS.items():
                value = p.get(field)
                if value is None or isinstance(value, bool) or not isinstance(value, expected):
                    errors.append(f"produto[{idx}] campo {field!r} inválido: {value!r}")
            if isinstance(p.get("price"), Number) and p["price"] < 0:
                errors.append(f"produto[{idx}] price negativo: {p['price']!r}")
            if isinstance(p.get("category"), str) and not p["category"].strip():
                errors.append(f"produto[{idx}] category vazia")

        if errors:
            self.log.error("%d violações de schema (mostrando 10): %s", len(errors), errors[:10])
            raise AirflowFailException(f"{len(errors)} violações de schema nos produtos")

        self.log.info("%d produtos válidos", len(self.produtos))
        return self.produtos
