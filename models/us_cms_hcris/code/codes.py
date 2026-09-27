"""Value labels for the dictionary-covered columns of us_cms_hcris.

Every entry is taken from CMS's own documentation, not inferred from the data:

* ``report_status_code``, ``utilization_code``, ``adr_vendor_code``,
  ``initial_report_indicator`` and ``last_report_indicator`` come from
  ``HCRIS_DataDictionary.csv`` in ``hospital2010-documentation.zip``.
* ``provider_control_type_code`` comes from CMS Publication 15-2 section
  4004.1, the instructions to Worksheet S-2 Part I line 21.

The value sets were then checked against all 182,903 published reports, and
they match: exactly the thirteen control types and the five report statuses CMS
documents appear in the data, and nothing else. The one exception is recorded
below.
"""

# {column: {code: (en, pt, es)}}
CODES: dict[str, dict[str, tuple[str, str, str]]] = {
    "form_version": {
        "2552-96": (
            "CMS Form 2552-96, filed for cost reporting periods before May 2010",
            "Formulário CMS 2552-96, apresentado para períodos anteriores a maio de 2010",
            "Formulario CMS 2552-96, presentado para períodos anteriores a mayo de 2010",
        ),
        "2552-10": (
            "CMS Form 2552-10, filed for cost reporting periods beginning on or "
            "after 1 May 2010",
            "Formulário CMS 2552-10, apresentado para períodos iniciados em ou "
            "após 1 de maio de 2010",
            "Formulario CMS 2552-10, presentado para períodos iniciados en o "
            "después del 1 de mayo de 2010",
        ),
    },
    "report_status_code": {
        "1": ("As submitted", "Como apresentado", "Como presentado"),
        "2": (
            "Settled without audit",
            "Homologado sem auditoria",
            "Homologado sin auditoría",
        ),
        "3": (
            "Settled with audit",
            "Homologado com auditoria",
            "Homologado con auditoría",
        ),
        "4": ("Reopened", "Reaberto", "Reabierto"),
        "5": ("Amended", "Retificado", "Rectificado"),
    },
    "provider_control_type_code": {
        "1": (
            "Voluntary nonprofit, church",
            "Sem fins lucrativos, religioso",
            "Sin fines de lucro, religioso",
        ),
        "2": (
            "Voluntary nonprofit, other",
            "Sem fins lucrativos, outros",
            "Sin fines de lucro, otros",
        ),
        "3": (
            "Proprietary, individual",
            "Com fins lucrativos, individual",
            "Con fines de lucro, individual",
        ),
        "4": (
            "Proprietary, corporation",
            "Com fins lucrativos, sociedade anônima",
            "Con fines de lucro, sociedad anónima",
        ),
        "5": (
            "Proprietary, partnership",
            "Com fins lucrativos, sociedade de pessoas",
            "Con fines de lucro, sociedad de personas",
        ),
        "6": (
            "Proprietary, other",
            "Com fins lucrativos, outros",
            "Con fines de lucro, otros",
        ),
        "7": (
            "Governmental, federal",
            "Governamental, federal",
            "Gubernamental, federal",
        ),
        "8": (
            "Governmental, city-county",
            "Governamental, municipal-condado",
            "Gubernamental, municipal-condado",
        ),
        "9": (
            "Governmental, county",
            "Governamental, condado",
            "Gubernamental, condado",
        ),
        "10": (
            "Governmental, state",
            "Governamental, estadual",
            "Gubernamental, estatal",
        ),
        "11": (
            "Governmental, hospital district",
            "Governamental, distrito hospitalar",
            "Gubernamental, distrito hospitalario",
        ),
        "12": (
            "Governmental, city",
            "Governamental, municipal",
            "Gubernamental, municipal",
        ),
        "13": (
            "Governmental, other",
            "Governamental, outros",
            "Gubernamental, otros",
        ),
    },
    "utilization_code": {
        "F": (
            "Full Medicare utilization",
            "Utilização integral do Medicare",
            "Utilización completa de Medicare",
        ),
        "L": (
            "Low Medicare utilization",
            "Baixa utilização do Medicare",
            "Baja utilización de Medicare",
        ),
        "N": (
            "No Medicare utilization",
            "Sem utilização do Medicare",
            "Sin utilización de Medicare",
        ),
    },
    "adr_vendor_code": {
        "2": ("Ernst & Young", "Ernst & Young", "Ernst & Young"),
        "3": ("KPMG", "KPMG", "KPMG"),
        "4": ("HFS", "HFS", "HFS"),
    },
    "initial_report_indicator": {
        "Y": (
            "Yes, the first cost report filed for this provider",
            "Sim, o primeiro relatório de custos apresentado por este prestador",
            "Sí, el primer informe de costos presentado por este proveedor",
        ),
        "N": ("No", "Não", "No"),
    },
    "last_report_indicator": {
        "Y": (
            "Yes, the final cost report filed for this provider",
            "Sim, o último relatório de custos apresentado por este prestador",
            "Sí, el último informe de costos presentado por este proveedor",
        ),
        "N": ("No", "Não", "No"),
        # 42 reports of 182,903 carry X. CMS documents the field as "Y, N or
        # blank", so this value has no published meaning; saying so is better
        # than guessing one, and the dictionary must cover it for the
        # custom_dictionary_coverage test to pass.
        "X": (
            "Value present in the source but not documented by CMS",
            "Valor presente na fonte, mas não documentado pela CMS",
            "Valor presente en la fuente, pero no documentado por CMS",
        ),
    },
}

# Which table each dictionary-covered column belongs to. A column appearing in
# two tables gets one dictionary row per table, which is what
# custom_dictionary_coverage looks for.
COVERED: dict[str, list[str]] = {
    "report": [
        "form_version",
        "provider_control_type_code",
        "report_status_code",
        "initial_report_indicator",
        "last_report_indicator",
        "adr_vendor_code",
        "utilization_code",
    ],
    "report_value": ["form_version"],
    "hospital_financial": ["form_version", "report_status_code"],
}

for _table, _cols in COVERED.items():
    for _col in _cols:
        assert _col in CODES, f"{_table}.{_col} has no code list"
