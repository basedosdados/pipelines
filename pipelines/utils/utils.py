"""
Utilitários gerais — logging, detecção de ambiente, helpers de string.
"""

import logging
from typing import Any


def log(msg: Any, level: str = "info") -> None:
    """Loga uma mensagem. Usa o logger do Prefect dentro de tasks/flows; logging padrão fora."""
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if level not in levels:
        raise ValueError(f"Nível de log inválido: {level}")
    try:
        from prefect.logging import get_run_logger

        get_run_logger().log(levels[level], str(msg))
    except Exception:
        logging.getLogger("pipelines").log(levels[level], str(msg))


def is_running_in_prod() -> bool:
    """Retorna True se o flow está rodando no work pool de prod (basedosdados)."""
    try:
        from prefect.runtime import flow_run

        return flow_run.work_pool_name == "basedosdados"
    except Exception:
        return False


def query_to_line(query: str) -> str:
    return " ".join(line.strip() for line in query.split("\n"))
