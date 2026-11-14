"""
Constant values for br_cgu_pessoal_executivo_federal.

Migrado pro pipeline em estágios (staged pipeline): check_update ->
extract_and_load -> build_and_promote. Tabela única (`terceirizados`).

O check (`crawl`) é leve de verdade: só faz scrape da página de listagem do
CGU (sem baixar nenhum CSV) e extrai a competência mais recente (AAAAMM) a
partir do nome dos arquivos. `extract_load_data` reaproveita o mesmo scrape
pra pegar a lista de URLs e baixa todos os CSVs publicados (histórico
completo), concatenando num único arquivo não particionado — o flow
monolítico antigo sempre sobrescrevia a tabela inteira
(`dump_mode="overwrite"`), comportamento preservado aqui.

Nunca houve registro de coverage/Table.Update pra essa tabela antes desta
migração (o flow antigo nunca chamava `register_table_materialization_task`,
`update_metadata` ficava sempre `False`).
"""

DATASET_ID = "br_cgu_pessoal_executivo_federal"
TERCEIRIZADOS_TABLE_ID = "terceirizados"

URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"
ROOT = "/tmp/data"
