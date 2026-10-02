import ast
from pathlib import Path

FLOW_PATH = Path(__file__).resolve().parents[1] / "flows.py"


def _flow_tree() -> ast.AST:
    """Parse the FUNDEB flow module without importing Prefect tasks."""
    return ast.parse(FLOW_PATH.read_text(encoding="utf-8"))


def test_flow_syncs_coverage_metadata_after_prod_materialization():
    """The FUNDEB flow must use the coverage-sync metadata task."""
    tree = _flow_tree()
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert "sync_table_coverage_task" in called_names
    assert "register_table_materialization_task" not in called_names
