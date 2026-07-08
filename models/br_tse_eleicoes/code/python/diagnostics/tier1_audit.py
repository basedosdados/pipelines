"""Tier 1 — static AST audit of the year-conditional schema mappings.

Extracts every raw->output column mapping site from ``sub/*.py`` without
executing the pipeline, resolves which election years each site covers, and
runs structural checks (coverage, duplicates, monotonicity).

Mapping patterns handled:
  A. dict literal  ``cols = {"v3": "ano", ...}``           (positional)
  B. paired lists  ``df = df[["v3", ...]].copy()`` followed by
                   ``df.columns = ["ano", ...]``           (positional)
  C. dict literal  ``keep_cols = {"sg_uf": "sigla_uf"}``   (named header)

Sites the extractor cannot parse are reported as ``extraction: failed`` and
must be transcribed into ``tier1_overrides.json``.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .spec import FUNC_SPECS, FuncSpec

V_RE = re.compile(r"^v\d+$")
CODE_DIR = Path(__file__).resolve().parent.parent  # .../code/python
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
OVERRIDES_PATH = Path(__file__).resolve().parent / "tier1_overrides.json"

# uf values to probe when a condition references the loop variable `uf`
_UF_PROBES = ("BR", "BRASIL", "TO", "ZZ", "AC", "SP")


@dataclass
class MappingSite:
    module: str
    function: str
    lineno: int
    kind: str  # "positional" | "named"
    mapping: dict[str, str]  # vN (or tse header) -> output column
    conditions: list[str]  # source text of the governing if-tests
    years: list[int]  # resolved years this site covers
    variant: str  # non-year discriminator (is_federal, uf == ...)
    extraction: str = "ast"
    issues: list[str] = field(default_factory=list)


@dataclass
class FunctionAudit:
    module: str
    function: str
    tables: list[str]
    family: str
    spec_years: list[int]
    sites: list[MappingSite]
    synthetic_cols: list[str]
    renames: dict[str, str]
    issues: list[str]


# ---------------------------------------------------------------------------
# condition evaluation
# ---------------------------------------------------------------------------


def _safe_eval(test: ast.expr, env: dict) -> bool:
    expr = ast.Expression(body=test)
    ast.fix_missing_locations(expr)
    return bool(
        eval(compile(expr, "<cond>", "eval"), {"__builtins__": {}}, env)
    )


def _frame_matches(test: ast.expr, want: bool, ano: int) -> tuple[bool, bool]:
    """Evaluate one if-frame for a year.

    Returns (matches, is_variant). A frame that references the `uf` loop
    variable matches if ANY probed uf satisfies it (the year constraint still
    binds); it is flagged as a variant. Unresolvable frames match and are
    flagged as variants.
    """
    base_env = {"ano": ano, "is_federal": ano % 4 == 2}
    try:
        return _safe_eval(test, base_env) is want, False
    except NameError:
        pass
    results = []
    for uf in _UF_PROBES:
        try:
            results.append(_safe_eval(test, {**base_env, "uf": uf}))
        except Exception:
            return True, True
    return (want in results), True


def _cond_source(src_lines: list[str], test: ast.expr, want: bool) -> str:
    seg = ast.get_source_segment("\n".join(src_lines), test) or "<cond>"
    seg = " ".join(seg.split())
    return seg if want else f"not ({seg})"


# ---------------------------------------------------------------------------
# AST walker
# ---------------------------------------------------------------------------


def _const_str_list(node: ast.expr) -> list[str] | None:
    """Return list of string constants if node is a literal list/tuple of them."""
    if isinstance(node, (ast.List, ast.Tuple)) and all(
        isinstance(e, ast.Constant) and isinstance(e.value, str)
        for e in node.elts
    ):
        return [e.value for e in node.elts]
    return None


def _const_str_dict(node: ast.expr) -> dict[str, str] | None:
    if isinstance(node, ast.Dict) and all(
        isinstance(k, ast.Constant)
        and isinstance(k.value, str)
        and isinstance(v, ast.Constant)
        and isinstance(v.value, str)
        for k, v in zip(node.keys, node.values, strict=False)
    ):
        return {
            k.value: v.value
            for k, v in zip(node.keys, node.values, strict=False)
        }
    return None


def _unwrap_copy(node: ast.expr) -> ast.expr:
    """Unwrap df[...].copy() -> df[...]."""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "copy"
    ):
        return node.func.value
    return node


def _select_list(node: ast.expr) -> list[str] | None:
    """Return the v-list from a `df[["v3", ...]]` subscript (or None)."""
    node = _unwrap_copy(node)
    if not isinstance(node, ast.Subscript):
        return None
    sl = node.slice
    if isinstance(sl, ast.Index):  # py<3.9 compat in stale ASTs
        sl = sl.value
    items = _const_str_list(sl)
    if items and all(V_RE.match(i) for i in items):
        return items
    return None


class _FunctionWalker:
    def __init__(self, src: str, module: str, fspec: FuncSpec):
        self.src_lines = src.splitlines()
        self.src = src
        self.module = module
        self.fspec = fspec
        self.sites: list[MappingSite] = []
        self.synthetic: list[str] = []
        self.renames: dict[str, str] = {}
        self.issues: list[str] = []

    # -- public ------------------------------------------------------------

    def walk(self, fn: ast.FunctionDef):
        self._walk_body(fn.body, cond_stack=[], pending={})

    # -- internals ----------------------------------------------------------

    def _resolve(self, cond_stack: list[tuple[ast.expr, bool]]):
        years, variants = [], set()
        for ano in self.fspec.years:
            ok = True
            for test, want in cond_stack:
                matches, is_variant = _frame_matches(test, want, ano)
                if is_variant:
                    variants.add(_cond_source(self.src_lines, test, want))
                if not matches:
                    ok = False
                    break
            if ok:
                years.append(ano)
        return years, " and ".join(sorted(variants))

    def _conditions(self, cond_stack) -> list[str]:
        return [_cond_source(self.src_lines, t, w) for t, w in cond_stack]

    def _add_site(self, lineno, kind, mapping, cond_stack):
        years, variant = self._resolve(cond_stack)
        site = MappingSite(
            module=self.module,
            function=self.fspec.function,
            lineno=lineno,
            kind=kind,
            mapping=mapping,
            conditions=self._conditions(cond_stack),
            years=years,
            variant=variant,
        )
        if kind == "positional":
            idx = [int(k[1:]) for k in mapping]
            if idx != sorted(idx):
                site.issues.append("non_monotonic_v_indices")
            if len(set(mapping.values())) != len(mapping):
                site.issues.append("duplicate_output_names")
        self.sites.append(site)

    def _walk_body(self, body, cond_stack, pending):
        for stmt in body:
            self._visit(stmt, cond_stack, pending)

    def _visit(self, stmt, cond_stack, pending):
        if isinstance(stmt, ast.If):
            self._walk_body(
                stmt.body, [*cond_stack, (stmt.test, True)], dict(pending)
            )
            self._walk_body(
                stmt.orelse, [*cond_stack, (stmt.test, False)], dict(pending)
            )
            return
        if isinstance(stmt, (ast.For, ast.While)):
            self._walk_body(stmt.body, cond_stack, pending)
            self._walk_body(stmt.orelse, cond_stack, pending)
            return
        if isinstance(stmt, ast.Try):
            self._walk_body(stmt.body, cond_stack, pending)
            for h in stmt.handlers:
                self._walk_body(h.body, cond_stack, pending)
            self._walk_body(stmt.finalbody, cond_stack, pending)
            return
        if isinstance(stmt, ast.With):
            self._walk_body(stmt.body, cond_stack, pending)
            return
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
            return

        target, value = stmt.targets[0], stmt.value

        # pattern A / C: dict literal mapping
        if isinstance(target, ast.Name):
            d = _const_str_dict(value)
            if d is not None and len(d) >= 4:
                if all(V_RE.match(k) for k in d):
                    self._add_site(stmt.lineno, "positional", d, cond_stack)
                elif target.id in ("keep_cols", "cols", "col_map"):
                    self._add_site(stmt.lineno, "named", d, cond_stack)
                return
            # pattern B step 1: v-list select
            sel = _select_list(value)
            if sel is not None:
                pending[target.id] = (sel, stmt.lineno)
                return

        # pattern B step 2: <name>.columns = [...]
        if (
            isinstance(target, ast.Attribute)
            and target.attr == "columns"
            and isinstance(target.value, ast.Name)
        ):
            names = _const_str_list(value)
            if names is None:  # ListComp header reset etc.
                pending.pop(target.value.id, None)
                return
            key = target.value.id
            if key in pending:
                sel, sel_lineno = pending.pop(key)
                if len(sel) == len(names):
                    self._add_site(
                        sel_lineno,
                        "positional",
                        dict(zip(sel, names, strict=False)),
                        cond_stack,
                    )
                else:
                    self.issues.append(
                        f"select/rename length mismatch at lines "
                        f"{sel_lineno}/{stmt.lineno} ({len(sel)} vs {len(names)})"
                    )
            else:
                self.issues.append(
                    f"unpaired .columns assignment at line {stmt.lineno}"
                )
            return

        # synthetic column: df["x"] = ...
        if (
            isinstance(target, ast.Subscript)
            and isinstance(target.slice, ast.Constant)
            and isinstance(target.slice.value, str)
        ):
            self.synthetic.append(target.slice.value)

        # renames: .rename(columns={...})
        for node in ast.walk(value):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "rename"
            ):
                for kw in node.keywords:
                    if kw.arg == "columns":
                        d = _const_str_dict(kw.value)
                        if d:
                            self.renames.update(d)


# ---------------------------------------------------------------------------
# overrides + checks
# ---------------------------------------------------------------------------


def _load_overrides() -> dict:
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text())
    return {}


def _apply_overrides(audit: FunctionAudit, overrides: dict):
    key = f"{audit.module}::{audit.function}"
    for ov in overrides.get(key, []):
        audit.sites.append(
            MappingSite(
                module=audit.module,
                function=audit.function,
                lineno=ov.get("lineno", 0),
                kind=ov.get("kind", "positional"),
                mapping=ov["mapping"],
                conditions=ov.get("conditions", []),
                years=ov["years"],
                variant=ov.get("variant", ""),
                extraction="override",
            )
        )


def _check_coverage(audit: FunctionAudit):
    """Every spec year must be covered by >=1 site; flag suspicious overlaps."""
    for ano in audit.spec_years:
        covering = [s for s in audit.sites if ano in s.years]
        if not covering:
            audit.issues.append(f"year {ano} not covered by any mapping site")
        # same year covered twice by sites with identical variant = overlap
        seen: dict[str, list[int]] = {}
        for s in covering:
            seen.setdefault(s.variant, []).append(s.lineno)
        for variant, linenos in seen.items():
            if len(linenos) > 1 and audit.function not in (
                # functions with legitimate multi-site years (main + extra file)
                "build_resultados_secao",  # special (1998,BR)/(2008,TO) re-read
                "_build_receitas_2014",  # main + supplementary file
                "build_candidatos",  # complementar merge 2024
            ):
                audit.issues.append(
                    f"year {ano} covered by {len(linenos)} sites "
                    f"(lines {linenos}, variant={variant!r})"
                )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def run(table: str | None = None) -> list[FunctionAudit]:
    overrides = _load_overrides()
    audits: list[FunctionAudit] = []
    by_module: dict[str, list[FuncSpec]] = {}
    for fs in FUNC_SPECS:
        if table in (None, "all") or table in fs.tables:
            by_module.setdefault(fs.module, []).append(fs)

    for module, fspecs in sorted(by_module.items()):
        src = (CODE_DIR / module).read_text()
        tree = ast.parse(src)
        fn_nodes = {
            n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
        }
        for fspec in fspecs:
            audit = FunctionAudit(
                module=module,
                function=fspec.function,
                tables=list(fspec.tables),
                family=fspec.family,
                spec_years=list(fspec.years),
                sites=[],
                synthetic_cols=[],
                renames={},
                issues=[],
            )
            fn = fn_nodes.get(fspec.function)
            if fn is None:
                audit.issues.append("function not found in module")
            else:
                walker = _FunctionWalker(src, module, fspec)
                walker.walk(fn)
                audit.sites = walker.sites
                audit.synthetic_cols = sorted(set(walker.synthetic))
                audit.renames = walker.renames
                audit.issues.extend(walker.issues)
            _apply_overrides(audit, overrides)
            _check_coverage(audit)
            audits.append(audit)

    out_dir = ARTIFACTS / "tier1"
    out_dir.mkdir(parents=True, exist_ok=True)
    by_mod: dict[str, list[FunctionAudit]] = {}
    for a in audits:
        by_mod.setdefault(a.module, []).append(a)
    for module, mod_audits in by_mod.items():
        out = out_dir / (Path(module).stem + ".json")
        out.write_text(
            json.dumps(
                [asdict(a) for a in mod_audits], indent=1, ensure_ascii=False
            )
        )
    return audits


def summarize(audits: list[FunctionAudit]) -> str:
    lines = []
    n_sites = sum(len(a.sites) for a in audits)
    n_issues = sum(len(a.issues) for a in audits) + sum(
        len(s.issues) for a in audits for s in a.sites
    )
    lines.append(
        f"tier1: {len(audits)} functions, {n_sites} mapping sites, "
        f"{n_issues} issue(s)"
    )
    for a in audits:
        flag = " !!" if a.issues else ""
        lines.append(
            f"  {a.module}::{a.function} — {len(a.sites)} sites, "
            f"years {a.spec_years[0]}-{a.spec_years[-1]}{flag}"
        )
        for s in a.sites:
            yrs = f"{s.years[0]}-{s.years[-1]}" if s.years else "NO YEARS"
            var = f" [{s.variant}]" if s.variant else ""
            si = f"  !! {', '.join(s.issues)}" if s.issues else ""
            lines.append(
                f"    L{s.lineno} {s.kind} {len(s.mapping)} cols -> "
                f"{yrs} ({len(s.years)}y){var}{si}"
            )
        for issue in a.issues:
            lines.append(f"    ISSUE: {issue}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(summarize(run()))
