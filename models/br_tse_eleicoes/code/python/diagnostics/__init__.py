"""Schema-validation diagnostics harness for the br_tse_eleicoes pipeline.

Validates the year-conditional positional column mappings in ``sub/*.py``
against the official TSE layout documentation (leiame) — schema-level only,
no table data is downloaded or scanned.

Tiers:
    1. Static AST audit of every builder's mapping blocks.
    2. Official TSE layout acquisition (local leiame or CDN partial-zip).
    3. Cross-check code mappings vs official layouts -> findings + report.
"""
