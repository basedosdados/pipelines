# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pipelines.utils.metadata.utils import (
    get_ids,
    create_update,
    extract_last_update,
    extract_last_date,
    get_first_date,
)
import re
from pipelines.utils.utils import log, get_credentials_from_secret
from typing import Tuple


class DjangoMetadataUpdater:
    def __init__(self, **kwargs):
        self.dataset_id = kwargs.get("dataset_id")
        self.table_id = kwargs.get("table_id")
        self.metadata_type = kwargs.get("metadata_type")
        self._last_date = kwargs.get("_last_date")
        self.date_format = kwargs.get("date_format", "yy-mm-dd")
        self.bq_last_update = kwargs.get("bq_last_update", True)
        self.bq_table_last_year_month = kwargs.get("bq_table_last_year_month", False)
        self.api_mode = kwargs.get("api_mode", "prod")
        self.billing_project_id = kwargs.get("billing_project_id", "basedosdados-dev")
        self.is_bd_pro = kwargs.get("is_bd_pro", False)
        self.is_free = kwargs.get("is_free", False)
        self.time_delta = kwargs.get("time_delta", 1)
        self.time_unit = kwargs.get("time_unit", "days")
        self.unidades_permitidas = {
            "years": "years",
            "months": "months",
            "weeks": "weeks",
            "days": "days",
        }

    def update_metadata(self):
        self._validate_args()
        (email, password) = self.get_credentials_utils(
            secret_path=f"api_user_{self.api_mode}"
        )
        ids = get_ids(
            self.dataset_id,
            self.table_id,
            email,
            password,
            is_bd_pro=self.is_bd_pro,
            is_free=self.is_free,
        )
        log(ids)
        if self.metadata_type == "DateTimeRange":
            if self.bq_last_update:
                if self.is_free and not self.is_bd_pro:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_update(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_free_coverage(last_date, ids, email, password)
                elif self.is_bd_pro and self.is_free:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_update(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_pro_and_free_coverage(last_date, ids, email, password)
                elif self.is_bd_pro and not self.is_free:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_update(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_pro_coverage(last_date, ids, email, password)
            elif self.bq_table_last_year_month:
                if self.is_free and not self.is_bd_pro:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_date(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_free_coverage(last_date, ids, email, password)
                elif self.is_bd_pro and self.is_free:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_date(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_pro_and_free_coverage(last_date, ids, email, password)
                elif self.is_bd_pro and not self.is_free:
                    log(
                        f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {self.table_id}.{self.dataset_id}"
                    )
                    last_date = extract_last_date(
                        self.dataset_id,
                        self.table_id,
                        self.date_format,
                        billing_project_id=self.billing_project_id,
                    )
                    self._update_pro_coverage(last_date, ids, email, password)
            else:
                if self.is_free and not self.is_bd_pro:
                    self._update_free_coverage(self._last_date, ids, email, password)
                elif self.is_bd_pro and self.is_free:
                    self._update_pro_and_free_coverage(
                        self._last_date, ids, email, password
                    )
                elif self.is_bd_pro and not self.is_free:
                    self._update_pro_coverage(self._last_date, ids, email, password)
                else:
                    raise ValueError("Invalid combination of flags")
        else:
            raise ValueError("Unsupported metadata type")

    def _validate_args(self):
        if self.time_unit not in self.unidades_permitidas:
            raise ValueError(
                f"Unidade temporal inválida. Escolha entre {', '.join(self.unidades_permitidas.keys())}"
            )

        if not isinstance(self.time_delta, int) or self.time_delta <= 0:
            raise ValueError("Defasagem deve ser um número inteiro positivo")

        accepted_billing_project_id = [
            "basedosdados-dev",
            "basedosdados",
            "basedosdados-staging",
        ]

        if self.billing_project_id not in accepted_billing_project_id:
            raise Exception(
                f"The given billing_project_id: {self.billing_project_id} is invalid. The accepted values are {accepted_billing_project_id}"
            )

    def get_credentials_utils(self, secret_path: str) -> Tuple[str, str]:
        """
        Returns the user and password for the given secret path.
        """
        log(f"Getting user and password for secret path: {secret_path}")
        tokens_dict = get_credentials_from_secret(secret_path)
        email = tokens_dict.get("email")
        password = tokens_dict.get("password")
        return email, password

    def _update_free_coverage(self, last_date, ids, email, password):
        # Restante da lógica para atualização de cobertura grátis
        resource_to_temporal_coverage_free = self.parse_temporal_coverage(
            temporal_coverage=f"{last_date}"
        )

        resource_to_temporal_coverage_free["coverage"] = ids.get("coverage_id")
        log(f"Mutation parameters: {resource_to_temporal_coverage_free}")

        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=resource_to_temporal_coverage_free,
            update=True,
            email=email,
            password=password,
            api_mode=self.api_mode,
        )

    def _update_pro_and_free_coverage(self, last_date, ids, email, password):
        delta_kwargs = {self.unidades_permitidas[self.time_unit]: self.time_delta}
        delta = relativedelta(**delta_kwargs)
        free_data = datetime.strptime(last_date, "%Y-%m-%d") - delta
        free_data = free_data.strftime("%Y-%m-%d")
        resource_to_temporal_coverage_pro = self.parse_temporal_coverage(
            temporal_coverage=f"{last_date}"
        )
        print(resource_to_temporal_coverage_pro)

        resource_to_temporal_coverage_free = self.parse_temporal_coverage(
            temporal_coverage=f"{free_data}"
        )
        # Restante da lógica para atualização de cobertura PRO e grátis
        resource_to_temporal_coverage_pro["coverage"] = ids.get("coverage_id_pro")
        log(f"Mutation parameters: {resource_to_temporal_coverage_pro}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=resource_to_temporal_coverage_pro,
            update=True,
            email=email,
            password=password,
            api_mode=self.api_mode,
        )
        resource_to_temporal_coverage_free = self.parse_temporal_coverage(
            temporal_coverage=f"{free_data}"
        )

        resource_to_temporal_coverage_free["coverage"] = ids.get("coverage_id")
        log(f"Mutation parameters: {resource_to_temporal_coverage_free}")

        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=resource_to_temporal_coverage_free,
            update=True,
            email=email,
            password=password,
            api_mode=self.api_mode,
        )

    def _update_pro_coverage(self, last_date, ids, email, password):
        resource_to_temporal_coverage_pro = self.parse_temporal_coverage(
            temporal_coverage=f"{last_date}"
        )
        # Restante da lógica para atualização de cobertura PRO e grátis
        resource_to_temporal_coverage_pro["coverage"] = ids.get("coverage_id_pro")
        log(f"Mutation parameters: {resource_to_temporal_coverage_pro}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=resource_to_temporal_coverage_pro,
            update=True,
            email=email,
            password=password,
            api_mode=self.api_mode,
        )

    def parse_temporal_coverage(self, temporal_coverage: str):
        padrao_ano = r"\d{4}\(\d{1,2}\)\d{4}"
        padrao_mes = r"\d{4}-\d{2}\(\d{1,2}\)\d{4}-\d{2}"
        padrao_semana = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"
        padrao_dia = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"

        if (
            re.match(padrao_ano, temporal_coverage)
            or re.match(padrao_mes, temporal_coverage)
            or re.match(padrao_semana, temporal_coverage)
            or re.match(padrao_dia, temporal_coverage)
        ):
            print("A data está no formato correto.")
        else:
            print("Aviso: A data não está no formato correto.")

        # Extrai as informações de data e intervalo da string
        if "(" in temporal_coverage:
            start_str, interval_str, end_str = re.split(r"[(|)]", temporal_coverage)
            if start_str == "" and end_str != "":
                start_str = end_str
            elif end_str == "" and start_str != "":
                end_str = start_str
        elif len(temporal_coverage) >= 4:
            start_str, interval_str, end_str = temporal_coverage, 1, temporal_coverage
        start_len = 0 if start_str == "" else len(start_str.split("-"))
        end_len = 0 if end_str == "" else len(end_str.split("-"))

        def parse_date(position, date_str, date_len):
            result = {}
            if date_len == 3:
                date = datetime.strptime(date_str, "%Y-%m-%d")
                result[f"{position}Year"] = date.year
                result[f"{position}Month"] = date.month
                result[f"{position}Day"] = date.day
            elif date_len == 2:
                date = datetime.strptime(date_str, "%Y-%m")
                result[f"{position}Year"] = date.year
                result[f"{position}Month"] = date.month
            elif date_len == 1:
                date = datetime.strptime(date_str, "%Y")
                result[f"{position}Year"] = date.year
            return result

        start_result = parse_date(
            position="start", date_str=start_str, date_len=start_len
        )
        end_result = parse_date(position="end", date_str=end_str, date_len=end_len)
        start_result.update(end_result)

        if interval_str != 0:
            start_result["interval"] = int(interval_str)

        return end_result


# Exemplo de uso
updater = DjangoMetadataUpdater(
    dataset_id="br_inmet_bdmep",
    table_id="microdados",
    metadata_type="DateTimeRange",
    bq_last_update=False,
    bq_table_last_year_month=False,
    billing_project_id="basedosdados-dev",
    api_mode="prod",
    date_format="yy",
    is_bd_pro=False,
    is_free=True,
    _last_date="2023"
    # Outros argumentos...
)

updater.update_metadata()
