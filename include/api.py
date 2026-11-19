"""Cliente mínimo da API de catálogo (FakeStore API)."""
from __future__ import annotations

import logging
from typing import Any

import requests

from include.config import API_BASE_URL, API_TIMEOUT_SECONDS

log = logging.getLogger(__name__)


def fetch_products() -> list[dict[str, Any]]:
    """GET /products. Levanta exceção em qualquer falha para o Airflow acionar o retry."""
    url = f"{API_BASE_URL}/products"
    try:
        resp = requests.get(url, timeout=API_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.error("Falha ao consultar %s: %s", url, exc)
        raise  # <- sem raise o erro seria engolido e o retry nunca dispararia

    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Resposta inesperada da API (tipo={type(payload).__name__}, vazia ou não-lista)")

    log.info("API retornou %d produtos", len(payload))
    return payload
