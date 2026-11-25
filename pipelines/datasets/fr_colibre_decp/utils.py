"""Pure download and cleaning helpers for fr_colibre_decp.

No Prefect imports here: the one-shot onboarding bootstrap under
``models/fr_colibre_decp/code/`` imports these same functions, so the transform
lives in exactly one place.

Source: the consolidated DECP file rebuilt daily by Colin Maudry from about 60
publishers (https://github.com/ColinMaudry/decp-processing). One row per
contract x amendment x awardee. Per the publisher, an amendment can only change
the awardees, the amount and the duration; every other field describes the
contract. The transform splits the file along that structure:

    marche        one row per contract (uid), as at its initial version
    modification  one row per contract x version (modification_id)
    titulaire     one row per contract x version x awardee

The source repeats contract-level fields on every row and occasionally carries
conflicting values for the same key (two source files describing one contract).
Each table keeps one row per key, chosen by the deterministic ordering in
``_TIEBREAK``: most recently published first, then source dataset and file.
"""

from __future__ import annotations

import csv
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import requests

from pipelines.datasets.fr_colibre_decp.constants import constants

ARCHITECTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "models"
    / "fr_colibre_decp"
    / "code"
    / "architecture"
)

# Rows per parquet row group. gcs.dump_header builds the staging table from row
# group 0 of the first file it reads, so a bounded group keeps that cheap.
ROW_GROUP_SIZE = 50_000

# Lower-case, accent-free, punctuation-free key used to unify spelling variants of
# one label ("MARCHE", "marché", "Marché"; "Appel d offres", typographic apostrophes).
_FOLD = (
    "trim(regexp_replace(lower(strip_accents(replace(replace({0}, 'œ', 'oe'), "
    "'Œ', 'oe'))), '[''\u2019_\\-\\s]+', ' ', 'g'))"
)

# Folded key -> canonical label. A key not listed keeps its original spelling, so a
# label the publisher adds later passes through instead of disappearing.
CANONICAL_LABELS: dict[str, dict[str, str]] = {
    "nature": {
        "marche": "Marché",
        "accord cadre": "Accord-cadre",
        "marche subsequent": "Marché subséquent",
        "marche de partenariat": "Marché de partenariat",
        "marche de defense ou de securite": "Marché de défense ou de sécurité",
        "delegation de service public": "Délégation de service public",
        "concession de service public": "Concession de service public",
    },
    "procedure": {
        "procedure adaptee": "Procédure adaptée",
        "appel d offres ouvert": "Appel d'offres ouvert",
        "appel d offres restreint": "Appel d'offres restreint",
        "procedure avec negociation": "Procédure avec négociation",
        "dialogue competitif": "Dialogue compétitif",
        "procedure concurrentielle avec negociation": (
            "Procédure concurrentielle avec négociation"
        ),
        "procedure negociee avec mise en concurrence prealable": (
            "Procédure négociée avec mise en concurrence préalable"
        ),
        "procedure negociee restreinte": "Procédure négociée restreinte",
        "marche passe sans publicite ni mise en concurrence prealable": (
            "Marché passé sans publicité ni mise en concurrence préalable"
        ),
        "marche negocie sans publicite ni mise en concurrence prealable": (
            "Marché négocié sans publicité ni mise en concurrence préalable"
        ),
        "marche public negocie sans publicite ni mise en concurrence prealable": (
            "Marché public négocié sans publicité ni mise en concurrence préalable"
        ),
    },
    "ccag": {
        "travaux": "Travaux",
        "fournitures courantes et services": "Fournitures courantes et services",
        "sans objet": "Sans objet",
        "prestations intellectuelles": "Prestations intellectuelles",
        "maitrise d oeuvre": "Maîtrise d'œuvre",
        "techniques de l information et de la communication": (
            "Techniques de l'information et de la communication"
        ),
        "marches industriels": "Marchés industriels",
    },
    "lieuExecution_typeCode": {
        "code postal": "Code postal",
        "code departement": "Code département",
        "code commune": "Code commune",
        "code region": "Code région",
        "code pays": "Code pays",
        "code arrondissement": "Code arrondissement",
        "code canton": "Code canton",
    },
    "titulaire_typeIdentifiant": {
        "siret": "SIRET",
        "tva": "TVA",
        "tva intracommunautaire": "TVA intracommunautaire",
        "hors ue": "HORS-UE",
        "ue": "UE",
        "irep": "IREP",
        "ridet": "RIDET",
        "tahiti": "TAHITI",
        "frw": "FRW",
        "frwf": "FRWF",
        "rci": "RCI",
        "autre": "Autre",
    },
}

_TIEBREAK = (
    "datePublicationDonnees desc nulls last, sourceDataset, sourceFile, "
    "titulaire_id nulls last, montant nulls last, dureeMois nulls last"
)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


def source_last_modified() -> datetime:
    """Return when the publisher last rebuilt decp.parquet (UTC-aware)."""
    response = requests.get(constants.RESOURCE_API.value, timeout=60)
    response.raise_for_status()
    stamp = response.json()["last_modified"]
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def download_decp(input_dir: Path) -> Path:
    """Stream decp.parquet (about 250 MB) into ``input_dir`` and return its path."""
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / "decp.parquet"
    partial = target.with_suffix(".parquet.part")
    with requests.get(
        constants.RESOURCE_URL.value, stream=True, timeout=600
    ) as response:
        response.raise_for_status()
        with partial.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1 << 20):
                handle.write(chunk)
    partial.replace(target)
    return target


# ---------------------------------------------------------------------------
# Architecture
# ---------------------------------------------------------------------------


def read_architecture(table: str) -> list[tuple[str, str]]:
    """(name, bigquery_type) pairs in architecture order."""
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as handle:
        return [
            (r["name"], r["bigquery_type"]) for r in csv.DictReader(handle)
        ]


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def _label(column: str) -> str:
    """SQL mapping a raw label column onto its canonical spelling."""
    folded = _FOLD.format(f'"{column}"')
    cases = " ".join(
        f"when {folded} = '{key}' then '{value.replace(chr(39), chr(39) * 2)}'"
        for key, value in CANONICAL_LABELS[column].items()
    )
    return f'case {cases} else "{column}" end'


def _num(expr: str) -> str:
    return f"case when isnan({expr}) then null else {expr} end"


def _percent(column: str) -> str:
    return _num(f'round(cast("{column}" as double) * 100, 6)')


def _point(lat: str, lon: str) -> str:
    return (
        f"case when {_num(lat)} is not null and {_num(lon)} is not null "
        f"then 'POINT(' || {lon} || ' ' || {lat} || ')' end"
    )


def _siren_from_siret(column: str) -> str:
    return f"case when regexp_full_match({column}, '[0-9]{{14}}') then left({column}, 9) end"


_PARTITION = {
    "ano": "k.ano",
    "mes": "k.mes",
    "id_marche": "r.uid",
}

_CONTRACT_AMOUNT = {
    "date_notification": "r.dateNotification",
    "date_publication_donnees": "r.datePublicationDonnees",
    "duree_mois": "r.dureeMois",
    "montant": _num("r.montant"),
    "montant_rationalise": _num("r.montant_rationalise"),
    "montant_anomalie": "r.montant_anomalie",
    "montant_anomalie_raisons": "r.montant_anomalie_raisons",
}

EXPRESSIONS: dict[str, dict[str, str]] = {
    "marche": {
        **_PARTITION,
        "id_marche_acheteur": "r.id",
        "id_accord_cadre": "r.idAccordCadre",
        "siret_acheteur": "r.acheteur_id",
        "siren_acheteur": _siren_from_siret("r.acheteur_id"),
        "nom_acheteur": "r.acheteur_nom",
        "categorie_acheteur": "r.acheteur_categorie",
        "labels_acheteur": "r.acheteur_labels",
        "code_commune_acheteur": "r.acheteur_commune_code",
        "code_departement_acheteur": "r.acheteur_departement_code",
        "code_region_acheteur": "r.acheteur_region_code",
        "geometria_acheteur": _point(
            "r.acheteur_latitude", "r.acheteur_longitude"
        ),
        "nature": "r.nature",
        "objet": "r.objet",
        "type_marche": "r.type",
        "code_cpv": "r.codeCPV",
        "procedure": "r.procedure",
        "techniques": "r.techniques",
        "modalites_execution": "r.modalitesExecution",
        **_CONTRACT_AMOUNT,
        "nombre_offres": "r.offresRecues",
        "forme_prix": "r.formePrix",
        "types_prix": "r.typesPrix",
        "attribution_avance": "r.attributionAvance",
        "taux_avance": _percent("tauxAvance"),
        "marche_innovant": "r.marcheInnovant",
        "considerations_sociales": "r.considerationsSociales",
        "considerations_environnementales": "r.considerationsEnvironnementales",
        "ccag": "r.ccag",
        "sous_traitance_declaree": "r.sousTraitanceDeclaree",
        "type_groupement_operateurs": "r.typeGroupementOperateurs",
        "origine_ue": _percent("origineUE"),
        "origine_france": _percent("origineFrance"),
        "code_lieu_execution": "r.lieuExecution_code",
        "type_code_lieu_execution": "r.lieuExecution_typeCode",
        "source_donnees": "r.sourceDataset",
        "fichier_source": "r.sourceFile",
    },
    "modification": {
        **_PARTITION,
        "id_modification": "r.modification_id",
        **_CONTRACT_AMOUNT,
        "donnees_actuelles": "r.donneesActuelles",
    },
    "titulaire": {
        **_PARTITION,
        "id_modification": "r.modification_id",
        "id_titulaire": "r.titulaire_id",
        "type_identifiant_titulaire": "r.titulaire_typeIdentifiant",
        "siren_titulaire": (
            "case when r.titulaire_typeIdentifiant = 'SIRET' then "
            f"{_siren_from_siret('r.titulaire_id')} end"
        ),
        "nom_titulaire": "r.titulaire_nom",
        "categorie_titulaire": "r.titulaire_categorie",
        "labels_titulaire": "r.titulaire_labels",
        "code_activite_titulaire": "r.titulaire_activite_code",
        "code_commune_titulaire": "r.titulaire_commune_code",
        "code_departement_titulaire": "r.titulaire_departement_code",
        "code_region_titulaire": "r.titulaire_region_code",
        "geometria_titulaire": _point(
            "r.titulaire_latitude", "r.titulaire_longitude"
        ),
        "distance_acheteur": "r.titulaire_distance",
    },
}

# One row per key, chosen by _TIEBREAK. ``titulaire`` keys an awardee by its
# identifier, or by its name when the source gives no identifier.
_KEYS = {
    "marche": "r.uid",
    "modification": "r.uid, r.modification_id",
    "titulaire": (
        "r.uid, r.modification_id, "
        "coalesce(r.titulaire_id, 'nom:' || r.titulaire_nom)"
    ),
}

_FILTERS = {
    "marche": "r.modification_id is not distinct from k.first_modification",
    "modification": "r.modification_id is not null",
    "titulaire": (
        "r.modification_id is not null "
        "and (r.titulaire_id is not null or r.titulaire_nom is not null)"
    ),
}


def _select_sql(table: str) -> str:
    arch = [name for name, _ in read_architecture(table)]
    exprs = EXPRESSIONS[table]
    if set(arch) != set(exprs):
        raise ValueError(
            f"{table}: architecture and transform disagree. "
            f"missing={sorted(set(arch) - set(exprs))} "
            f"extra={sorted(set(exprs) - set(arch))}"
        )
    cols = ",\n        ".join(
        f"cast({exprs[name]} as varchar) as {name}" for name in arch
    )
    return f"""
    select
        {cols}
    from (
        select r.*, row_number() over (
            partition by {_KEYS[table]} order by {_TIEBREAK}
        ) as _rank
        from raw r
        join kept k on r.uid = k.uid
        where {_FILTERS[table]}
    ) r
    join kept k on r.uid = k.uid
    where r._rank = 1
    """


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"query returned no value: {sql}")
    return int(row[0])


def clean_decp(
    source: Path,
    output_dir: Path,
    first_year: int | None = None,
    memory_limit: str = "3GB",
) -> dict[str, int]:
    """Split decp.parquet into the three tables as hive-partitioned parquet.

    Writes ``<output_dir>/<table>/ano=YYYY/data_0.parquet``. Every column is
    written as STRING, in architecture order, with ``ano`` carried by the
    partition path: staging is all-STRING by house convention and the dbt models
    ``safe_cast`` each column. Returns the row count per table.
    """
    first_year = first_year or constants.FIRST_YEAR.value
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"set memory_limit = '{memory_limit}'")
    con.execute(f"set temp_directory = '{output_dir.parent / 'duckdb_tmp'}'")
    con.execute("set preserve_insertion_order = false")

    labels = {c: _label(c) for c in CANONICAL_LABELS}
    replace = ", ".join(f'{expr} as "{col}"' for col, expr in labels.items())
    con.execute(
        f"create temp table raw as select * replace ({replace}) "
        f"from read_parquet('{source}')"
    )
    # The contract's partition comes from its initial version, so all of a
    # contract's rows land in one partition across the three tables.
    con.execute(
        f"""
        create temp table kept as
        with first_version as (
            select uid, min(modification_id) as first_modification
            from raw where uid is not null group by uid
        ),
        initial as (
            select r.uid, f.first_modification, r.dateNotification,
                row_number() over (partition by r.uid order by {_TIEBREAK}) as _rank
            from raw r join first_version f on r.uid = f.uid
            where r.modification_id is not distinct from f.first_modification
        )
        select uid, first_modification,
            year(dateNotification) as ano, month(dateNotification) as mes
        from initial
        where _rank = 1
            and year(dateNotification) between {first_year} and year(current_date)
        """
    )

    counts: dict[str, int] = {}
    for table in constants.TABLES.value:
        target = output_dir / table
        if target.exists():
            shutil.rmtree(target)
        con.execute(f"create temp table out_{table} as {_select_sql(table)}")
        counts[table] = _scalar(con, f"select count(*) from out_{table}")
        con.execute(
            f"""
            copy out_{table} to '{target}' (
                format parquet, compression snappy, partition_by (ano),
                row_group_size {ROW_GROUP_SIZE}
            )
            """
        )
        con.execute(f"drop table out_{table}")
    con.close()
    return counts


def source_max_date(output_dir: Path) -> str:
    """Latest initial-notification month in the cleaned marche table, "YYYY-MM"."""
    con = duckdb.connect()
    value = _scalar(
        con,
        "select max(cast(ano as int) * 100 + cast(mes as int)) "
        f"from read_parquet('{Path(output_dir) / 'marche'}/*/*.parquet', "
        "hive_partitioning = true)",
    )
    con.close()
    return f"{value // 100:04d}-{value % 100:02d}"
