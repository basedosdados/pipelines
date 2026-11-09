"""
Constant values for br_cgu_cartao_pagamento.
"""

DATASET_ID = "br_cgu_cartao_pagamento"

# As 3 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline), substituindo o antigo `_run_cgu_cartao_pagamento` monolítico
# (que segue existindo em `pipelines/shared/cgu/flows.py`, compartilhado com
# outros datasets br_cgu_* ainda não migrados). Cada tabela baixa seu próprio
# ZIP do Portal da Transparência de forma independente — sem download
# compartilhado entre elas.
#
# `br_cgu_cartao_pagamento__dicionario` não é uma 4ª tabela deste pipeline:
# não tem entrada em `pipelines.shared.cgu.constants.constants.TABELAS` nem
# task de check_update/extract_and_load — é lida direto de
# `br_cgu_cartao_pagamento_staging.dicionario` no dbt (ver
# `models/br_cgu_cartao_pagamento/br_cgu_cartao_pagamento__dicionario.sql`),
# mantida fora deste fluxo mesmo que o teste `custom_dictionary_coverage` das
# 3 tabelas abaixo referencie ela via `ref()`.
MICRODADOS_GOVERNO_FEDERAL_TABLE_ID = "microdados_governo_federal"
MICRODADOS_COMPRAS_CENTRALIZADAS_TABLE_ID = "microdados_compras_centralizadas"
MICRODADOS_DEFESA_CIVIL_TABLE_ID = "microdados_defesa_civil"
