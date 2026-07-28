"""
MetadataClient — camada de transporte da interação com o backend da BD.

Cliente único para os recursos do backend que as pipelines escrevem e leem:
`Coverage.DateTimeRange`, `Update` (FK Table e FK RawDataSource) e `Poll` (FK
RawDataSource). Concentra toda a costura de GraphQL num só lugar:

- uma instância por flow, parametrizada por `env` (dev/staging/prod) — sem URL
  hardcoded;
- autentica uma vez e reaproveita o token em todas as mutations;
- resolve `Entity` dinamicamente pelo slug, em vez de UUIDs embutidos no código;
- verifica o campo `errors` de cada mutation e levanta `BackendMutationError`;
- todo GraphQL passa por `self._backend`, injetável nos testes.

Não conhece regra de negócio: recebe DTOs já validados (`dto.py`) e os envia.
"""

from __future__ import annotations

import datetime
from typing import Literal

import basedosdados as bd

from pipelines.constants import constants
from pipelines.utils.metadata.dto import (
    DateTimeRangeInput,
    PollInput,
    RawSourceUpdateInput,
    TableUpdateInput,
    _compose_end_date,
    _to_iso8601,
)
from pipelines.utils.vault import get_credentials_from_secret

_API_URL_BY_ENV = {
    "prod": constants.API_URL.value["prod"],
    "staging": constants.API_URL.value["staging"],
    # `dev` escreve no backend de staging (API_URL não tem chave 'dev').
    "dev": constants.API_URL.value["staging"],
}


class BackendMutationError(RuntimeError):
    """Mutation GraphQL retornou `errors` não-vazio."""

    def __init__(self, mutation_class: str, errors: object):
        super().__init__(f"{mutation_class} falhou: {errors}")
        self.mutation_class = mutation_class
        self.errors = errors


class MetadataClient:
    def __init__(
        self,
        env: Literal["dev", "prod", "staging"] = "dev",
        billing_project: str | None = None,
        *,
        backend: bd.Backend | None = None,
        entity_ids: dict[str, str] | None = None,
    ):
        if env not in _API_URL_BY_ENV:
            raise ValueError(f"env inválido: {env!r}. Use dev/prod/staging.")
        self.env = env
        self.billing_project = billing_project
        self._backend = backend or bd.Backend(graphql_url=_API_URL_BY_ENV[env])
        self._token: str | None = None
        self._entity_ids: dict[str, str] = dict(entity_ids or {})

    # ----------------------------------------------------------------- transporte
    def _execute(
        self,
        query: str,
        variables: dict | None = None,
        headers: dict | None = None,
    ):
        """Costura única de GraphQL. Os testes substituem `self._backend`."""
        return self._backend._execute_query(
            query, variables=variables or {}, headers=headers
        )

    def _query_id(
        self, query_class: str, query_parameters: dict
    ) -> str | None:
        """Resolve no máximo um nó para os parâmetros dados e devolve seu `_id`
        (ou None). Levanta se a query casar mais de um objeto."""
        _filter = ", ".join(query_parameters)
        keys = [p.replace("$", "").split(":")[0] for p in query_parameters]
        values = list(query_parameters.values())
        _input = ", ".join(f"{k}:${k}" for k in keys)
        query = (
            f"query({_filter}) {{ {query_class}({_input}) "
            f"{{ edges {{ node {{ _id }} }} }} }}"
        )
        variables = dict(zip(keys, values, strict=False))
        response = self._execute(query, variables)
        nodes = response[query_class]["items"]
        if len(nodes) > 1:
            raise ValueError(
                f"{query_class}: mais de um nó encontrado para {variables}; "
                "refine os parâmetros para retornar um único objeto."
            )
        if not nodes:
            return None
        return nodes[0]["_id"]

    def _mutate(self, mutation_class: str, payload: dict) -> str:
        """Executa uma mutation `CreateUpdate*` com token cacheado e checagem do
        campo `errors`; devolve o id do nó criado/atualizado."""
        _classe = mutation_class.replace("CreateUpdate", "").lower()
        query = f"""
            mutation($input: {mutation_class}Input!) {{
                {mutation_class}(input: $input) {{
                    errors {{ field, messages }},
                    clientMutationId,
                    {_classe} {{ id }}
                }}
            }}
        """
        response = self._execute(
            query, {"input": payload}, self._auth_headers()
        )
        result = response[mutation_class]
        if result.get("errors"):
            raise BackendMutationError(mutation_class, result["errors"])
        node_id = result[_classe]["id"]
        return node_id.split(":")[1] if ":" in node_id else node_id

    # ---------------------------------------------------------------------- auth
    def _authenticate(self) -> str:
        if self._token is None:
            self._token = self._fetch_token()
        return self._token

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._authenticate()}"}

    def _fetch_token(self) -> str:
        cred_mode = "prod" if self.env == "prod" else "staging"
        credentials = get_credentials_from_secret(
            secret_path=f"api_user_{cred_mode}"
        )
        mutation = """
            mutation ($email: String!, $password: String!) {
                tokenAuth(email: $email, password: $password) { token }
            }
        """
        response = self._execute(
            mutation,
            {
                "email": credentials["email"],
                "password": credentials["password"],
            },
        )
        return response["tokenAuth"]["token"]

    def _entity_id(self, kind: Literal["day", "month"]) -> str:
        """Resolve o id da `Entity` pelo slug.

        Em produção o id é buscado no backend (`allEntity`) e cacheado. Nos
        testes unitários o id é injetado via `entity_ids=` no construtor.
        """
        if kind in self._entity_ids:
            return self._entity_ids[kind]
        resolved = self._query_id("allEntity", {"$slug: String": kind})
        if not resolved:
            raise ValueError(
                f"Entity slug {kind!r} não encontrada no backend."
            )
        self._entity_ids[kind] = resolved
        return resolved

    # --------------------------------------------------------------- resolvers
    def get_table_id(self, dataset_id: str, table_id: str) -> str:
        return self._backend._get_table_id_from_name(
            gcp_dataset_id=dataset_id, gcp_table_id=table_id
        )

    def _raw_source_id(self, dataset_id: str, table_id: str) -> str | None:
        table_pk = self.get_table_id(dataset_id, table_id)
        return self._query_id("allRawdatasource", {"$tables_Id: ID": table_pk})

    # ------------------------------------------------------------------ leitura
    def get_table_status(self, dataset_id: str, table_id: str) -> str | None:
        table_pk = self.get_table_id(dataset_id, table_id)
        query = """query($table_id: ID) {
            allTable(id: $table_id) { edges { node { status { slug } } } }
        }"""
        response = self._execute(query, {"table_id": table_pk})
        nodes = response["allTable"]["items"]
        return nodes[0]["status"]["slug"] if nodes else None

    def get_coverage_ids(self, dataset_id: str, table_id: str):
        """Lê os ids de Coverage free (is_closed=False) e pro (is_closed=True) e
        devolve um `policy.CoverageIds`. A validação de topologia fica a cargo da
        policy, não aqui.
        """
        from pipelines.utils.metadata.policy import CoverageIds

        table_pk = self.get_table_id(dataset_id, table_id)
        pro = self._query_id(
            "allCoverage",
            {"$table_Id: ID": table_pk, "$isClosed: Boolean": True},
        )
        free = self._query_id(
            "allCoverage",
            {"$table_Id: ID": table_pk, "$isClosed: Boolean": False},
        )
        return CoverageIds(free=free, pro=pro)

    def get_table_update_latest(
        self, dataset_id: str, table_id: str
    ) -> datetime.date | None:
        """Lê o campo `latest` do `Update` vinculado à `Table`."""
        table_pk = self.get_table_id(dataset_id, table_id)
        return self._read_update_latest("$table_Id: ID", table_pk, "table_Id")

    def get_raw_source_update_latest(
        self, dataset_id: str, table_id: str
    ) -> datetime.date | None:
        """Lê o campo `latest` do `Update` vinculado ao `RawDataSource`."""
        rds_id = self._raw_source_id(dataset_id, table_id)
        if rds_id is None:
            return None
        return self._read_update_latest(
            "$rawDataSource_Id: ID", rds_id, "rawDataSource_Id"
        )

    def get_table_coverage_end(
        self, dataset_id: str, table_id: str
    ) -> datetime.date | None:
        """Fim da cobertura temporal da `Table` — o período mais recente que a
        tabela de fato contém.

        Toma o **máximo** entre os `DateTimeRange` de todas as coberturas: num
        `part_bdpro` a free termina em `free_end` e a pro em `source_end`, então
        só a pro reflete a extensão real; num `all_free` há um range só e o
        máximo é ele mesmo.

        É a fonte de verdade para "o que já temos": vem de
        `bq.read_max_date`, gravado por `register_table_materialization`. Os
        `Update` são escrituração e podem divergir do dado (visto em prod:
        `br_ibge_ipca.mes_brasil` com cobertura até 2026-05 e
        `RawDataSource.Update.latest` em 2026-06-01).

        Devolve ``None`` se a tabela não tem cobertura datada — o chamador trata
        como "nunca materializado".
        """
        table_pk = self.get_table_id(dataset_id, table_id)
        query = """query($table_Id: ID) {
            allCoverage(table_Id: $table_Id) {
                edges { node { datetimeRanges { edges { node {
                    endYear, endMonth, endDay
                } } } } }
            }
        }"""
        response = self._execute(query, {"table_Id": table_pk})
        ends = [
            end
            for coverage in response["allCoverage"]["items"]
            for dtr in (coverage.get("datetimeRanges") or {}).get("items", [])
            if (end := _compose_end_date(dtr)) is not None
        ]
        return max(ends) if ends else None

    def _read_update_latest(
        self, filter_decl: str, value: str, field: str
    ) -> datetime.date | None:
        query = f"""query({filter_decl}) {{
            allUpdate({field}: ${field}) {{ edges {{ node {{ id, latest }} }} }}
        }}"""
        response = self._execute(query, {field: value})
        nodes = response["allUpdate"]["items"]
        if not nodes or not nodes[0].get("latest"):
            return None
        return datetime.datetime.strptime(
            nodes[0]["latest"][:10], "%Y-%m-%d"
        ).date()

    # ------------------------------------------------------ escrita (1 por entidade)
    def upsert_raw_source_poll(
        self, dataset_id: str, table_id: str, *, latest
    ) -> str:
        """Cria/atualiza o `Poll` ligado ao `RawDataSource`."""
        rds_id = self._raw_source_id(dataset_id, table_id)
        existing = self._query_id("allPoll", {"$rawDataSource_Id: ID": rds_id})
        if existing:
            return self._mutate(
                "CreateUpdatePoll",
                {"id": existing, "latest": _to_iso8601(latest)},
            )
        dto = PollInput(
            rawDataSource=rds_id, latest=latest, entity=self._entity_id("day")
        )
        return self._mutate("CreateUpdatePoll", dto.model_dump())

    def upsert_raw_source_update(
        self, dataset_id: str, table_id: str, *, latest
    ) -> str:
        """Cria/atualiza o `Update` ligado ao `RawDataSource`."""
        rds_id = self._raw_source_id(dataset_id, table_id)
        existing = self._query_id(
            "allUpdate", {"$rawDataSource_Id: ID": rds_id}
        )
        if existing:
            return self._mutate(
                "CreateUpdateUpdate",
                {"id": existing, "latest": _to_iso8601(latest)},
            )
        dto = RawSourceUpdateInput(
            rawDataSource=rds_id,
            latest=latest,
            frequency=1,
            entity=self._entity_id("month"),
        )
        return self._mutate(
            "CreateUpdateUpdate", dto.model_dump(exclude_none=True)
        )

    def upsert_table_update(
        self, dataset_id: str, table_id: str, *, latest
    ) -> str:
        """Cria/atualiza o `Update` ligado à `Table`."""
        table_pk = self.get_table_id(dataset_id, table_id)
        existing = self._query_id("allUpdate", {"$table_Id: ID": table_pk})
        if existing:
            return self._mutate(
                "CreateUpdateUpdate",
                {"id": existing, "latest": _to_iso8601(latest)},
            )
        dto = TableUpdateInput(
            table=table_pk,
            latest=latest,
            frequency=1,
            entity=self._entity_id("month"),
        )
        return self._mutate(
            "CreateUpdateUpdate", dto.model_dump(exclude_none=True)
        )

    def upsert_coverage_datetime_range(self, dto: DateTimeRangeInput) -> str:
        """Cria/atualiza um `Coverage.DateTimeRange` a partir de um DTO validado."""
        existing = self._query_id(
            "allDatetimerange", {"$coverage_Id: ID": dto.coverage}
        )
        payload = dto.model_dump(exclude_none=True)
        if existing:
            payload["id"] = existing
        return self._mutate("CreateUpdateDateTimeRange", payload)
