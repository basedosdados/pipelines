"""Register us_ssa_beneficiaries metadata in the Data Basis backend.

    uv run python models/us_ssa_beneficiaries/code/metadata.py --env staging
    uv run python models/us_ssa_beneficiaries/code/metadata.py --env prod
    uv run python models/us_ssa_beneficiaries/code/metadata.py --env prod --publish

Idempotent: every record is looked up before it is written and existing ids are
reused. ``create_update_*`` creates a duplicate when called without an id, so a
naive re-run silently doubles observation levels, coverages and updates.

Datasets are created ``under_review``. Publishing is a separate, deliberate act:
on staging before the PR, so a reviewer sees the dataset as it will appear; on
prod only after the PR merges, table-approve materialises the tables and they
are verified. ``--publish`` flips the status and does nothing else.
"""

from __future__ import annotations

# ruff: noqa: E402  (the MCP server lives outside the repo, so sys.path comes first)
import argparse
import datetime
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_ROOT = Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
HERE = Path(__file__).resolve().parent
for path in (str(REPO_ROOT), str(MCP_ROOT), str(HERE)):
    sys.path.insert(0, path)

import server  # type: ignore[import-not-found]  # pyrefly: ignore[import-error]
from build_columns_json import columns_json  # type: ignore[import-not-found]

DATASET_SLUG = "beneficiaries"
GCP_DATASET = "us_ssa_beneficiaries"
LAST_YEAR = 2025

# Tag slugs are Portuguese on staging and English on prod for the same records,
# so they are resolved per environment rather than hardcoded once.
TAG_SLUGS = {
    "staging": [
        "previdencia_social",
        "aposentadoria",
        "renda",
        "assistencia_social",
    ],
    "dev": [
        "previdencia_social",
        "aposentadoria",
        "renda",
        "assistencia_social",
    ],
    "prod": [
        "social_security",
        "retirement",
        "income",
        "social_assistance",
        "previdencia_social",
        "aposentadoria",
        "renda",
        "assistencia_social",
    ],
}
THEME_SLUGS = ["economics", "population"]

# table -> (geographic entity, first year); None means no coverage spec
TABLES: dict[str, tuple[str | None, int | None]] = {
    "oasdi_county": ("county", 1999),
    "oasdi_state": ("state", 1999),
    "oasdi_population_share": ("state", 1999),
    "ssi_county": ("county", 1998),
    "ssi_state": ("state", 1998),
    "dicionario": (None, None),
}

# One raw data source per table: the client's _raw_source_id raises when a table
# has two or more, which would break the recurring pipeline's poll outright.
SOURCE_OF = {
    "oasdi_county": "oasdi",
    "oasdi_state": "oasdi",
    "oasdi_population_share": "oasdi",
    "ssi_county": "ssi",
    "ssi_state": "ssi",
}

ORG = dict(
    slug="social_security_administration",
    name_pt="Administração da Previdência Social dos Estados Unidos",
    name_en="Social Security Administration",
    name_es="Administración del Seguro Social de Estados Unidos",
    description_pt="Agência federal independente dos Estados Unidos responsável pela "
    "administração do seguro social: aposentadoria, pensão por morte e invalidez "
    "(OASDI) e o benefício assistencial Supplemental Security Income (SSI). O Office "
    "of Retirement and Disability Policy publica as estatísticas oficiais do programa.",
    description_en="Independent federal agency of the United States responsible for "
    "administering social insurance: retirement, survivors and disability insurance "
    "(OASDI) and the means-tested Supplemental Security Income (SSI) program. Its "
    "Office of Retirement and Disability Policy publishes the official program "
    "statistics.",
    description_es="Agencia federal independiente de Estados Unidos responsable de "
    "administrar el seguro social: jubilación, sobrevivientes e invalidez (OASDI) y el "
    "programa asistencial Supplemental Security Income (SSI). Su Office of Retirement "
    "and Disability Policy publica las estadísticas oficiales del programa.",
    website="https://www.ssa.gov/policy/",
)

DATASET = dict(
    slug=DATASET_SLUG,
    name_pt="Beneficiários do OASDI e do SSI por estado e condado",
    name_en="OASDI and SSI Beneficiaries by State and County",
    name_es="Beneficiarios del OASDI y del SSI por estado y condado",
    description_pt="Número de beneficiários e valor dos benefícios pagos pelos dois "
    "principais programas de transferência de renda dos Estados Unidos, por condado e "
    "por estado, em dezembro de cada ano. O OASDI (Old-Age, Survivors, and Disability "
    "Insurance) é o seguro social contributivo de aposentadoria, pensão por morte e "
    "invalidez, coberto desde 1999 e detalhado por tipo de benefício, faixa etária e "
    "sexo. O SSI (Supplemental Security Income) é o benefício assistencial não "
    "contributivo, coberto desde 1998 e detalhado por categoria de elegibilidade, "
    "faixa etária e recebimento simultâneo de OASDI. Os dados vêm dos arquivos de "
    "série temporal achatada publicados pelo Office of Retirement and Disability "
    "Policy da SSA.",
    description_en="Number of beneficiaries and amount of benefits paid by the two "
    "main income-transfer programs of the United States, by county and by state, in "
    "December of each year. OASDI (Old-Age, Survivors, and Disability Insurance) is "
    "the contributory social insurance program covering retirement, survivors and "
    "disability, available from 1999 and broken down by type of benefit, age group and "
    "sex. SSI (Supplemental Security Income) is the means-tested assistance program, "
    "available from 1998 and broken down by eligibility category, age group and "
    "concurrent receipt of OASDI. The data come from the flattened time-series files "
    "published by SSA's Office of Retirement and Disability Policy.",
    description_es="Número de beneficiarios y monto de los beneficios pagados por los "
    "dos principales programas de transferencia de renta de Estados Unidos, por "
    "condado y por estado, en diciembre de cada año. El OASDI (Old-Age, Survivors, and "
    "Disability Insurance) es el seguro social contributivo de jubilación, "
    "sobrevivientes e invalidez, disponible desde 1999 y desagregado por tipo de "
    "beneficio, grupo de edad y sexo. El SSI (Supplemental Security Income) es el "
    "beneficio asistencial no contributivo, disponible desde 1998 y desagregado por "
    "categoría de elegibilidad, grupo de edad y recepción simultánea de OASDI. Los "
    "datos provienen de los archivos de serie temporal aplanada publicados por la "
    "Office of Retirement and Disability Policy de la SSA.",
)

SOURCES = {
    "oasdi": dict(
        name_pt="OASDI Beneficiaries by State and County — série temporal achatada",
        name_en="OASDI Beneficiaries by State and County — flattened time series",
        name_es="OASDI Beneficiaries by State and County — serie temporal aplanada",
        description_pt="Arquivos JSON que concatenam as tabelas 1 a 5 de todas as "
        "edições anuais de OASDI Beneficiaries by State and County, da edição de 1999 "
        "até a mais recente. Cada arquivo traz os próprios metadados de esquema: "
        "dimensões, medidas com unidade e tipo, fontes e notas de rodapé. Os arquivos "
        "são regerados a cada nova edição anual, incorporando correções posteriores à "
        "publicação.",
        description_en="JSON files concatenating tables 1 through 5 of every annual "
        "edition of OASDI Beneficiaries by State and County, from the 1999 edition to "
        "the latest. Each file carries its own schema metadata: dimensions, measures "
        "with unit and type, sources and footnotes. The files are regenerated with "
        "every new annual edition, incorporating post-release corrections.",
        description_es="Archivos JSON que concatenan las tablas 1 a 5 de todas las "
        "ediciones anuales de OASDI Beneficiaries by State and County, desde la "
        "edición de 1999 hasta la más reciente. Cada archivo trae sus propios "
        "metadatos de esquema: dimensiones, medidas con unidad y tipo, fuentes y notas "
        "al pie. Los archivos se regeneran con cada nueva edición anual, incorporando "
        "correcciones posteriores a la publicación.",
        url="https://www.ssa.gov/policy/docs/statcomps/oasdi_sc/flat-series.html",
    ),
    "ssi": dict(
        name_pt="SSI Recipients by State and County — série temporal achatada",
        name_en="SSI Recipients by State and County — flattened time series",
        name_es="SSI Recipients by State and County — serie temporal aplanada",
        description_pt="Arquivos JSON que concatenam as tabelas 1 a 3 de todas as "
        "edições anuais de SSI Recipients by State and County, da edição de 1998 até a "
        "mais recente. Cada arquivo traz os próprios metadados de esquema. A tabela 4, "
        "sobre suplementação estadual opcional administrada pelo governo federal, não "
        "tem arquivo achatado por variar de estrutura entre edições.",
        description_en="JSON files concatenating tables 1 through 3 of every annual "
        "edition of SSI Recipients by State and County, from the 1998 edition to the "
        "latest. Each file carries its own schema metadata. Table 4, on federally "
        "administered optional state supplementation, has no flat file because its "
        "structure varies across editions.",
        description_es="Archivos JSON que concatenan las tablas 1 a 3 de todas las "
        "ediciones anuales de SSI Recipients by State and County, desde la edición de "
        "1998 hasta la más reciente. Cada archivo trae sus propios metadatos de "
        "esquema. La tabla 4, sobre suplementación estatal opcional administrada por "
        "el gobierno federal, no tiene archivo aplanado porque su estructura varía "
        "entre ediciones.",
        url="https://www.ssa.gov/policy/docs/statcomps/ssi_sc/flat-series.html",
    ),
}

TABLE_TEXT = {
    "oasdi_county": (
        "OASDI por condado",
        "OASDI by county",
        "OASDI por condado",
        "Número de beneficiários do OASDI em current-payment status e valor total dos "
        "benefícios pagos, por condado, ano, tipo de benefício, faixa etária e sexo. "
        "Formato longo: cada linha é uma combinação de condado, ano e categoria. Os "
        "nove tipos de benefício somam o total; as linhas por sexo são um recorte da "
        "população de 65 anos ou mais e não devem ser somadas aos tipos de benefício. "
        "As linhas de total estadual publicadas pela fonte estão na tabela oasdi_state.",
        "Number of OASDI beneficiaries in current-payment status and total amount of "
        "benefits paid, by county, year, type of benefit, age group and sex. Long "
        "format: each row is one combination of county, year and category. The nine "
        "benefit types sum to the total; the rows by sex are a cut of the population "
        "aged 65 or older and must not be added to the benefit types. The state-total "
        "rows published by the source are in the oasdi_state table.",
        "Número de beneficiarios del OASDI en current-payment status y monto total de "
        "los beneficios pagados, por condado, año, tipo de beneficio, grupo de edad y "
        "sexo. Formato largo: cada fila es una combinación de condado, año y "
        "categoría. Los nueve tipos de beneficio suman el total; las filas por sexo "
        "son un recorte de la población de 65 años o más y no deben sumarse a los "
        "tipos de beneficio. Las filas de total estatal publicadas por la fuente están "
        "en la tabla oasdi_state.",
    ),
    "oasdi_state": (
        "OASDI por estado ou área",
        "OASDI by state or area",
        "OASDI por estado o área",
        "Número de beneficiários do OASDI em current-payment status e valor total dos "
        "benefícios pagos, por estado ou área, ano, tipo de benefício, faixa etária e "
        "sexo. Inclui os territórios, o total nacional ('All areas') e as categorias "
        "residuais 'Other', 'Foreign countries' e 'Unknown'; filtre por state_id não "
        "nulo para obter apenas estados e territórios. A contagem de beneficiários de "
        "2010, ausente do arquivo da fonte, foi recuperada das linhas de total "
        "estadual da tabela por condado.",
        "Number of OASDI beneficiaries in current-payment status and total amount of "
        "benefits paid, by state or area, year, type of benefit, age group and sex. "
        "Includes the territories, the national total ('All areas') and the residual "
        "categories 'Other', 'Foreign countries' and 'Unknown'; filter on a non-null "
        "state_id for states and territories only. The 2010 beneficiary counts, absent "
        "from the source file, were recovered from the state-total rows of the county "
        "table.",
        "Número de beneficiarios del OASDI en current-payment status y monto total de "
        "los beneficios pagados, por estado o área, año, tipo de beneficio, grupo de "
        "edad y sexo. Incluye los territorios, el total nacional ('All areas') y las "
        "categorías residuales 'Other', 'Foreign countries' y 'Unknown'; filtre por "
        "state_id no nulo para obtener solo estados y territorios. El recuento de "
        "beneficiarios de 2010, ausente del archivo de la fuente, se recuperó de las "
        "filas de total estatal de la tabla por condado.",
    ),
    "oasdi_population_share": (
        "Participação do OASDI na população",
        "OASDI share of the population",
        "Participación del OASDI en la población",
        "População residente estimada e percentual dela que recebe benefícios do "
        "OASDI, por estado ou área e ano, para a população total e para a população de "
        "65 anos ou mais. As estimativas populacionais são do Census Bureau, referidas "
        "a 1º de julho do ano. O percentual não pode ser recalculado a partir dos "
        "valores estaduais.",
        "Estimated resident population and the percentage of it receiving OASDI "
        "benefits, by state or area and year, for the total population and for the "
        "population aged 65 or older. The population estimates are from the Census "
        "Bureau, as of July 1 of the year. The percentage cannot be recomputed from "
        "the state values.",
        "Población residente estimada y el porcentaje de ella que recibe beneficios "
        "del OASDI, por estado o área y año, para la población total y para la "
        "población de 65 años o más. Las estimaciones poblacionales son del Census "
        "Bureau, referidas al 1 de julio del año. El porcentaje no puede recalcularse "
        "a partir de los valores estatales.",
    ),
    "ssi_county": (
        "SSI por condado",
        "SSI by county",
        "SSI por condado",
        "Número de recebedores do SSI e valor total dos pagamentos, por condado, ano, "
        "categoria de elegibilidade, faixa etária e recebimento simultâneo de OASDI. "
        "Formato longo. O valor dos pagamentos só é publicado para o total do condado, "
        "de modo que as demais categorias têm o valor nulo. Contagens suprimidas para "
        "evitar a identificação de indivíduos aparecem como nulo, com o motivo em "
        "recipient_count_note, nunca como zero.",
        "Number of SSI recipients and total amount of payments, by county, year, "
        "eligibility category, age group and concurrent receipt of OASDI. Long format. "
        "The payment amount is published only for the county total, so the other "
        "categories carry a null amount. Counts suppressed to avoid identifying "
        "individuals appear as null, with the reason in recipient_count_note, never as "
        "zero.",
        "Número de beneficiarios del SSI y monto total de los pagos, por condado, año, "
        "categoría de elegibilidad, grupo de edad y recepción simultánea de OASDI. "
        "Formato largo. El monto de los pagos solo se publica para el total del "
        "condado, por lo que las demás categorías tienen el valor nulo. Los recuentos "
        "suprimidos para evitar la identificación de individuos aparecen como nulo, "
        "con el motivo en recipient_count_note, nunca como cero.",
    ),
    "ssi_state": (
        "SSI por estado ou área",
        "SSI by state or area",
        "SSI por estado o área",
        "Número de recebedores do SSI e valor total dos pagamentos, por estado ou "
        "área, ano, categoria de elegibilidade, faixa etária e recebimento simultâneo "
        "de OASDI. Inclui os territórios e o total nacional ('All areas'); filtre por "
        "state_id não nulo para obter apenas estados e territórios. O valor dos "
        "pagamentos existe para seis das sete categorias, todas menos o recorte de "
        "recebimento simultâneo de OASDI.",
        "Number of SSI recipients and total amount of payments, by state or area, "
        "year, eligibility category, age group and concurrent receipt of OASDI. "
        "Includes the territories and the national total ('All areas'); filter on a "
        "non-null state_id for states and territories only. The payment amount exists "
        "for six of the seven categories, all but the concurrent-OASDI cut.",
        "Número de beneficiarios del SSI y monto total de los pagos, por estado o "
        "área, año, categoría de elegibilidad, grupo de edad y recepción simultánea de "
        "OASDI. Incluye los territorios y el total nacional ('All areas'); filtre por "
        "state_id no nulo para obtener solo estados y territorios. El monto de los "
        "pagos existe para seis de las siete categorías, todas menos el recorte de "
        "recepción simultánea de OASDI.",
    ),
    "dicionario": (
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Dicionário de valores para as colunas codificadas das tabelas do conjunto: "
        "tipo de benefício, categoria de elegibilidade, faixa etária, sexo, "
        "recebimento simultâneo de OASDI, grupo populacional e os motivos pelos quais "
        "um valor está nulo.",
        "Dictionary of values for the coded columns of the dataset's tables: type of "
        "benefit, eligibility category, age group, sex, concurrent receipt of OASDI, "
        "population group and the reasons a value is null.",
        "Diccionario de valores para las columnas codificadas de las tablas del "
        "conjunto: tipo de beneficio, categoría de elegibilidad, grupo de edad, sexo, "
        "recepción simultánea de OASDI, grupo poblacional y los motivos por los cuales "
        "un valor es nulo.",
    ),
}


def tool(name: str) -> Any:
    """Return an MCP tool's plain function, unwrapped from its FastMCP decorator.

    Returns ``Any``: the MCP module is untyped and the tools are resolved by
    name, so a precise signature is not recoverable here.
    """
    fn = getattr(server, name)
    return getattr(fn, "fn", fn)


def bare(identifier: str | None) -> str | None:
    """Strip the Relay ``XNode:`` prefix the API returns on every id."""
    if not identifier:
        return None
    return identifier.split(":", 1)[1] if ":" in identifier else identifier


TABLE_STATE_Q = """query($id: ID!) { allTable(id: $id) { edges { node {
  observationLevels { edges { node { id entity { id } } } }
  coverages { edges { node { id datetimeRanges { edges { node { id } } } } } }
  updates { edges { node { id } } }
  cloudTables { edges { node { id } } }
} } } }"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["staging", "prod", "dev"]
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="only flip the dataset status to published",
    )
    args = parser.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"
    now = datetime.datetime.now().replace(microsecond=0).isoformat()

    lookup, get_ds = tool("lookup_id"), tool("get_dataset")
    uid = lambda cat, slug: lookup(category=cat, slug=slug, env=env)["id"]  # noqa: E731

    status_published = uid("status", "published")
    existing = get_ds(slug=DATASET_SLUG, env=env)

    if args.publish:
        if not existing.get("found"):
            print(f"dataset {DATASET_SLUG!r} not found on {env}")
            return 1
        node = server._gql(
            """query($id: ID!) { allDataset(id: $id) { edges { node { slug namePt
               nameEn nameEs descriptionPt descriptionEn descriptionEs
               organizations { edges { node { id } } } themes { edges { node { id } } }
               tags { edges { node { id } } } } } } }""",
            {"id": existing["id"]},
            env=env,
            auth=False,
        )["allDataset"]["edges"][0]["node"]
        # No partial updates: every required field is re-passed or it is blanked.
        tool("create_update_dataset")(
            id=existing["id"],
            slug=node["slug"],
            name_pt=node["namePt"],
            name_en=node["nameEn"],
            name_es=node["nameEs"],
            description_pt=node["descriptionPt"],
            description_en=node["descriptionEn"],
            description_es=node["descriptionEs"],
            organization_ids=[
                bare(e["node"]["id"]) for e in node["organizations"]["edges"]
            ],
            theme_ids=[bare(e["node"]["id"]) for e in node["themes"]["edges"]],
            tag_ids=[bare(e["node"]["id"]) for e in node["tags"]["edges"]],
            status_id=status_published,
            env=env,
        )
        print(f"{DATASET_SLUG} published on {env}")
        return 0

    print(f"=== {env} (cloud tables -> {gcp_project}) ===")
    area_us = uid("area", "us")
    status_review = uid("status", "under_review")
    licence = uid("license", "cc0")
    availability = uid("availability", "online")
    language_en = uid("language", "en")
    account = tool("get_authenticated_account")(env=env)["id"]
    entity = {k: uid("entity", k) for k in ("county", "state", "year")}
    themes = [uid("theme", s) for s in THEME_SLUGS]

    tags, missing = [], []
    for slug in TAG_SLUGS[env]:
        try:
            tags.append(uid("tag", slug))
        except Exception:
            missing.append(slug)
    tags = list(dict.fromkeys(tags))
    if missing:
        print(f"  tags not on {env} (skipped): {missing}")
    print(f"  {len(tags)} tags, {len(themes)} themes")

    org = tool("create_update_organization")(
        **ORG, area_id=area_us, env=env, id=_existing_org_id(ORG["slug"], env)
    )
    print(f"  organization {org['slug']}")

    dataset = tool("create_update_dataset")(
        **DATASET,
        organization_ids=[bare(org["id"])],
        theme_ids=themes,
        tag_ids=tags,
        status_id=status_review,
        env=env,
        id=existing.get("id") if existing.get("found") else None,
    )
    dataset_id = bare(dataset["id"])
    print(f"  dataset {dataset['slug']} ({dataset_id})")

    src_ids = {}
    # get_raw_data_sources returns a list of {id, name, url}; key on the url,
    # which is stable and language-independent, not the localised name.
    known = {
        r["url"]: r["id"]
        for r in tool("get_raw_data_sources")(
            dataset_slug=DATASET_SLUG, env=env
        )
    }
    for key, spec in SOURCES.items():
        r = tool("create_update_raw_data_source")(
            dataset_id=dataset_id,
            **spec,
            license_id=licence,
            availability_id=availability,
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            language_ids=[language_en],
            env=env,
            id=bare(known.get(spec["url"])),
        )
        src_ids[key] = bare(r["id"])
        print(f"  raw source {key}")

    status_table = status_published
    refreshed = get_ds(slug=DATASET_SLUG, env=env)
    table_ids = {s: t["id"] for s, t in refreshed.get("tables", {}).items()}
    created_ids: dict[str, str] = {}

    for slug, (geo, first_year) in TABLES.items():
        pt, en, es, dpt, den, des = TABLE_TEXT[slug]
        t = tool("create_update_table")(
            slug=slug,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=status_table,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            env=env,
            id=bare(table_ids.get(slug)),
        )
        tid = bare(t["id"])
        if tid is None:
            raise RuntimeError(f"{slug}: backend returned no table id")
        created_ids[slug] = tid
        tool("bulk_upsert_columns")(
            table_id=tid, columns_json=columns_json(slug), env=env
        )

        node = server._gql(TABLE_STATE_Q, {"id": tid}, env=env, auth=False)[
            "allTable"
        ]["edges"][0]["node"]
        ols = {
            bare(e["node"]["entity"]["id"]): bare(e["node"]["id"])
            for e in node["observationLevels"]["edges"]
        }
        covs = [
            (
                bare(e["node"]["id"]),
                [
                    bare(r["node"]["id"])
                    for r in e["node"]["datetimeRanges"]["edges"]
                ],
            )
            for e in node["coverages"]["edges"]
        ]
        ups = [bare(e["node"]["id"]) for e in node["updates"]["edges"]]
        cts = [bare(e["node"]["id"]) for e in node["cloudTables"]["edges"]]

        tool("create_update_cloud_table")(
            table_id=tid,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=slug,
            id=cts[0] if cts else None,
            env=env,
        )

        if first_year is None or geo is None:
            print(f"  table {slug:24s} columns + cloud table (no coverage)")
            continue

        cols = {
            e["node"]["name"]: bare(e["node"]["id"])
            for e in server._gql(
                "query($id: ID!) { allColumn(table_Id: $id) { edges { node { id name } } } }",
                {"id": tid},
                env=env,
                auth=False,
            )["allColumn"]["edges"]
        }
        for col, ent in [(f"{geo}_id", entity[geo]), ("year", entity["year"])]:
            oid = ols.get(ent) or bare(
                tool("create_update_observation_level")(
                    table_id=tid, entity_id=ent, env=env
                )["id"]
            )
            ols[ent] = oid
            # update_column's booleans default False and would clear the flag,
            # so is_partition is re-passed for the partition column.
            tool("update_column")(
                column_id=cols[col],
                column_name=col,
                table_id=tid,
                observation_level_id=oid,
                is_partition=(col == "year"),
                env=env,
            )
        cid, ranges = (
            covs[0]
            if covs
            else (
                bare(
                    tool("create_update_coverage")(
                        table_id=tid, area_id=area_us, env=env
                    )["id"]
                ),
                [],
            )
        )
        tool("create_update_datetime_range")(
            coverage_id=cid,
            start_year=first_year,
            end_year=LAST_YEAR,
            interval=1,
            id=ranges[0] if ranges else None,
            env=env,
        )
        # The table Update is a wall clock: when WE last refreshed it. lag=1 year
        # because SSA publishes the December edition the following northern summer.
        tool("create_update_update")(
            entity_id=entity["year"],
            frequency=1,
            lag=1,
            latest=now,
            table_id=tid,
            id=ups[0] if ups else None,
            env=env,
        )
        print(
            f"  table {slug:24s} columns + 2 OLs + coverage {first_year}-{LAST_YEAR}"
        )

    # Deferred: link each table to its single raw source, re-passing every
    # field because create_update_* does no partial update. The table ids come
    # from the loop above rather than a fresh get_dataset, which costs ~25s a
    # call and was being made once per link.
    for slug, key in SOURCE_OF.items():
        pt, en, es, dpt, den, des = TABLE_TEXT[slug]
        tool("create_update_table")(
            id=created_ids[slug],
            slug=slug,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=status_table,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            raw_data_source_ids=[src_ids[key]],
            env=env,
        )
    print(f"  linked {len(SOURCE_OF)} tables to their raw source")

    # The SOURCE Update's `latest` is what SSA published -- the max coverage date
    # of the newest edition -- not today. Today would claim SSA released data today.
    for sid in src_ids.values():
        tool("create_update_update")(
            entity_id=entity["year"],
            frequency=1,
            latest=f"{LAST_YEAR}-12-01T00:00:00",
            raw_data_source_id=sid,
            env=env,
        )
    print("  source updates recorded")
    print(f"\nDone on {env}. Dataset is under_review; publish separately.")
    return 0


def _existing_org_id(slug: str, env: str) -> str | None:
    """Return the organization's id if it already exists, else None."""
    try:
        return tool("lookup_id")(category="organization", slug=slug, env=env)[
            "id"
        ]
    except Exception:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
