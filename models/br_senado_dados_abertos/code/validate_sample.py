"""Probe real column sets + temporal extents for all T1 tables (small slices)."""

import json
import sys

import pandas as pd

sys.path.insert(0, ".")
from senado_api import _as_list, dig, get_json

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 200)


def show(name, df):
    print(f"\n{'=' * 72}\n### {name}  rows={len(df)}")
    print("columns:", list(df.columns))
    if len(df):
        print(df.head(2).to_dict("records")[0])


# ---- 6. partido (envelope) ----------------------------------------------------
d = get_json("/composicao/lista/partidos")
parts = _as_list(dig(d, "ListaPartidos", "Partidos", "Partido"))
show("partido", pd.DataFrame(parts))

# ---- 7. bloco (envelope, drop Membros) ---------------------------------------
d = get_json("/composicao/lista/blocos")
blocos = _as_list(dig(d, "ListaBlocoParlamentar", "Blocos", "Bloco"))
show(
    "bloco",
    pd.DataFrame(
        [{k: v for k, v in b.items() if k != "Membros"} for b in blocos]
    ),
)

# ---- 8. lideranca (flat) ------------------------------------------------------
d = get_json("/composicao/lideranca")
show("lideranca", pd.DataFrame(_as_list(d)))

# ---- 10. mesa SF (deep nested → one row per Cargo) ---------------------------
d = get_json("/composicao/mesaSF")
cols = _as_list(dig(d, "MesaSenado", "Colegiados", "Colegiado"))
rows = []
for c in cols:
    for cg in _as_list(dig(c, "Cargos", "Cargo")):
        rows.append(
            {
                "CodigoColegiado": c.get("CodigoColegiado"),
                "SiglaColegiado": c.get("SiglaColegiado"),
                "NomeColegiado": c.get("NomeColegiado"),
                **{
                    k: v
                    for k, v in cg.items()
                    if not isinstance(v, (list, dict))
                },
            }
        )
show("mesa", pd.DataFrame(rows))

# ---- 9. comissao (try colegiados, then a type) -------------------------------
for path in ["/comissao/lista/colegiados", "/comissao/lista/tiposColegiado"]:
    d = get_json(path)
    print(
        f"\ncomissao probe {path}: type={type(d).__name__} keys="
        f"{list(d.keys()) if isinstance(d, dict) else 'n/a'}"
    )
    if d:
        print(json.dumps(d, ensure_ascii=False)[:500])

# ---- 1. senador (legislature range → identity) -------------------------------
d = get_json("/senador/lista/legislatura/40/57")
pls = _as_list(
    dig(d, "ListaParlamentarLegislatura", "Parlamentares", "Parlamentar")
)
ident = [p.get("IdentificacaoParlamentar", {}) for p in pls]
df_sen = pd.DataFrame(ident).drop_duplicates(subset=["CodigoParlamentar"])
show("senador (leg 40-57, deduped)", df_sen)

# ---- 2/3. votacao + votacao_parlamentar (2024 full year) ---------------------
d = get_json("/votacao", {"dataInicio": "2024-01-01", "dataFim": "2024-12-31"})
d = _as_list(d)
print(f"\nvotacao 2024: {len(d)} vote-level records")
vp = []
for v in d:
    for voto in _as_list(v.get("votos")):
        vp.append(
            {
                "codigoSessaoVotacao": v.get("codigoSessaoVotacao"),
                "dataSessao": v.get("dataSessao"),
                **voto,
            }
        )
show(
    "votacao (vote-level, drop votos)",
    pd.DataFrame(
        [
            {
                k: x
                for k, x in v.items()
                if k not in ("votos", "informeLegislativo")
            }
            for v in d
        ]
    ),
)
show("votacao_parlamentar (exploded)", pd.DataFrame(vp))

# ---- 4. orientacao bancada (2024) --------------------------------------------
d = get_json("/plenario/votacao/orientacaoBancada/20240101/20241231")
vots = _as_list(dig(d, "votacoes")) if isinstance(d, dict) else _as_list(d)
ob = []
for v in vots:
    for o in _as_list(v.get("orientacoesLideranca")):
        ob.append(
            {
                "sequencialVotacao": v.get("sequencialVotacao"),
                "codigoVotacaoSve": v.get("codigoVotacaoSve"),
                "siglaTipoMateria": v.get("siglaTipoMateria"),
                "numeroMateria": v.get("numeroMateria"),
                "anoMateria": v.get("anoMateria"),
                "dataInicioVotacao": v.get("dataInicioVotacao"),
                **o,
            }
        )
show("votacao_orientacao_bancada (exploded)", pd.DataFrame(ob))

# ---- 5. processo (2024, check cap) -------------------------------------------
d = get_json("/processo", {"ano": 2024})
d = _as_list(d)
print(f"\nprocesso 2024 (no sigla filter): {len(d)} records")
show(
    "processo",
    pd.DataFrame(
        [
            {k: v for k, v in p.items() if not isinstance(v, (list, dict))}
            for p in d
        ]
    ),
)
