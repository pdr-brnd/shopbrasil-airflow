"""Lógica pura de métricas (sem dependência de Airflow -> facilmente testável)."""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterable


def _money(value: float | Decimal) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def extract_categories(products: Iterable[dict[str, Any]]) -> list[str]:
    """Categorias distintas e ordenadas, derivadas dos próprios dados.

    Novas categorias na API entram no fan-out sem nenhuma alteração de código.
    """
    return sorted({p["category"] for p in products})


def compute_category_metrics(products: Iterable[dict[str, Any]], category: str) -> dict[str, Any]:
    prices = [float(p["price"]) for p in products if p["category"] == category]
    if not prices:
        raise ValueError(f"Nenhum produto encontrado para a categoria {category!r}")
    return {
        "category": category,
        "product_count": len(prices),
        "avg_price": _money(sum(prices) / len(prices)),
        "min_price": _money(min(prices)),
        "max_price": _money(max(prices)),
    }
